pub mod blocks;
pub mod config;
mod generated;
pub mod polls;
pub mod rpc;

pub use config::Config;

use rpc::{get_block, get_block_results, get_chain_list, get_chain_params, get_head};

use blocks::Block;
use config::ChainParams;
use log::{debug, error, info, warn};
use polls::{Poll, PollData};
use std::collections::BTreeMap;
use std::sync::mpsc;

#[derive(Debug, Clone, Copy)]
pub enum Height {
    Latest,
    Specific(u64),
}

pub enum IoCommand {
    FetchBlock(Height),
    FetchBlockResults(u64, Vec<PollData>),
    FetchChainList,
    FetchChainParams(String),
    FetchHead,
    #[allow(dead_code)]
    Shutdown,
}

pub enum IoResult {
    Block(u64, Block),
    BlockResults(u64, rpc::BlockResults, Vec<PollData>),
    FetchError(Height, String),
    ChainList(Vec<String>),
    ChainParams(config::ChainParams),
    Head(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VoteResultType {
    Agreed,
    Disagreed,
    Missed,
}

impl PartialOrd for VoteResultType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VoteResultType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (VoteResultType::Agreed, VoteResultType::Agreed) => std::cmp::Ordering::Equal,
            (VoteResultType::Agreed, _) => std::cmp::Ordering::Less,
            (VoteResultType::Disagreed, VoteResultType::Agreed) => std::cmp::Ordering::Greater,
            (VoteResultType::Disagreed, VoteResultType::Disagreed) => std::cmp::Ordering::Equal,
            (VoteResultType::Disagreed, VoteResultType::Missed) => std::cmp::Ordering::Less,
            (VoteResultType::Missed, VoteResultType::Missed) => std::cmp::Ordering::Equal,
            (VoteResultType::Missed, _) => std::cmp::Ordering::Greater,
        }
    }
}

impl std::fmt::Display for VoteResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VoteResultType::Agreed => write!(f, "agreed"),
            VoteResultType::Disagreed => write!(f, "disagreed"),
            VoteResultType::Missed => write!(f, "missed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, aetos::Label)]
pub struct VoteResult {
    pub broadcaster: String,
    pub chain: String,
    pub result: VoteResultType,
}

#[aetos::metrics(prefix = "axelar_monitor")]
pub struct MetricsSnapshot {
    #[counter(help = "Total number of fetch errors")]
    pub fetch_error_count: u64,
    #[counter(help = "Current chain height")]
    pub chain_height: u64,
    #[counter(help = "Broadcaster vote results by chain and outcome")]
    pub broadcaster_votes: BTreeMap<VoteResult, u64>,
    #[counter(help = "Last block height that was processed")]
    pub last_processed_height: u64,
}

pub enum ProcessingMessage {
    IoResult(IoResult),
    QueryMetrics(mpsc::Sender<MetricsSnapshot>),
    Shutdown,
}

pub enum IoResponse {
    SendMessage(ProcessingMessage),
    Shutdown,
}

pub struct ProcessingState {
    pub chain_height: u64,
    pub fetch_error_count: u64,
    pub chain_params: BTreeMap<String, ChainParams>,
    pub polls: BTreeMap<u64, Poll>,
    pub chain_tip: u64,
    pub last_processed_height: u64,
    pub vote_results: BTreeMap<VoteResult, u64>,
}

impl ProcessingState {
    pub fn new(config: &Config) -> Self {
        let mut chain_params: BTreeMap<String, ChainParams> = BTreeMap::new();
        for chain_param in &config.chain_params {
            chain_params.insert(chain_param.name.to_lowercase(), chain_param.clone());
        }

        let mut vote_results = BTreeMap::new();
        for broadcaster in &config.broadcaster {
            for chain_param in &config.chain_params {
                for result_type in [
                    VoteResultType::Agreed,
                    VoteResultType::Disagreed,
                    VoteResultType::Missed,
                ] {
                    let key = VoteResult {
                        broadcaster: broadcaster.name.clone(),
                        chain: chain_param.name.to_lowercase(),
                        result: result_type,
                    };
                    vote_results.insert(key, 0);
                }
            }
        }

        ProcessingState {
            chain_height: 0,
            fetch_error_count: 0,
            chain_params,
            polls: BTreeMap::new(),
            chain_tip: 0,
            last_processed_height: 0,
            vote_results,
        }
    }
}

fn analyze_poll_completion(
    poll: &Poll,
    config: &Config,
    vote_results: &mut BTreeMap<VoteResult, u64>,
) {
    let mut tx_id_counts: BTreeMap<&str, u64> = BTreeMap::new();
    for vote in &poll.votes {
        *tx_id_counts.entry(&vote.tx_id).or_insert(0) += 1;
    }

    let majority_tx_id = tx_id_counts
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(tx_id, _)| *tx_id);

    let sender_to_broadcaster: BTreeMap<String, &str> = config
        .broadcaster
        .iter()
        .map(|bc| (bc.address.clone(), bc.name.as_str()))
        .collect();

    let voters: std::collections::HashSet<&str> = poll
        .votes
        .iter()
        .filter_map(|vote| sender_to_broadcaster.get(&vote.sender_id).copied())
        .collect();

    for broadcaster in &config.broadcaster {
        if !voters.contains(broadcaster.name.as_str()) {
            let key = VoteResult {
                broadcaster: broadcaster.name.clone(),
                chain: poll.data.chain.to_lowercase(),
                result: VoteResultType::Missed,
            };
            *vote_results.entry(key).or_insert(0) += 1;
            warn!(
                "Missed vote: broadcaster={} chain={} poll_id={}",
                broadcaster.name, poll.data.chain, poll.poll_id
            );
        }
    }

    if let Some(majority) = majority_tx_id {
        for vote in &poll.votes {
            if let Some(broadcaster_name) = sender_to_broadcaster.get(&vote.sender_id) {
                let result_type = if vote.tx_id == majority {
                    VoteResultType::Agreed
                } else {
                    VoteResultType::Disagreed
                };

                let key = VoteResult {
                    broadcaster: broadcaster_name.to_string(),
                    chain: vote.chain.to_lowercase(),
                    result: result_type.clone(),
                };
                *vote_results.entry(key).or_insert(0) += 1;

                if vote.tx_id != majority {
                    warn!(
                        "Disagreed vote: broadcaster={} chain={} poll_id={} voted_tx={} majority_tx={}",
                        broadcaster_name, vote.chain, vote.poll_id, vote.tx_id, majority
                    );
                }
            }
        }
    }
}

pub enum ProcessingResponse {
    SendIoCommand(IoCommand),
    SendMetricsSnapshot(mpsc::Sender<MetricsSnapshot>, MetricsSnapshot),
    Shutdown,
}

pub fn process_single_message(
    msg: ProcessingMessage,
    state: &mut ProcessingState,
    config: &Config,
) -> Vec<ProcessingResponse> {
    match msg {
        ProcessingMessage::IoResult(result) => match result {
            IoResult::FetchError(h, e) => {
                state.fetch_error_count += 1;
                error!("Error fetching at height {:?}: {}", h, e);
                vec![]
            }
            IoResult::ChainList(chains) => {
                info!(
                    "Chain list received, fetching params for {} chains",
                    chains.len()
                );
                let mut responses = Vec::new();
                for chain in chains {
                    if !state.chain_params.contains_key(&chain) {
                        responses.push(ProcessingResponse::SendIoCommand(
                            IoCommand::FetchChainParams(chain),
                        ));
                    }
                }
                responses
            }
            IoResult::Block(height, block) => {
                state.chain_height = std::cmp::max(state.chain_height, height);
                state.last_processed_height = height;
                info!(
                    "got block height is {}, chain height is {}",
                    height, state.chain_height
                );

                let expiring_poll_ids: Vec<_> = state
                    .polls
                    .iter()
                    .filter(|(_, poll)| poll.data.expiry_height <= height as u64)
                    .map(|(id, _)| *id)
                    .collect();

                for poll_id in expiring_poll_ids {
                    if let Some(poll) = state.polls.get(&poll_id) {
                        analyze_poll_completion(poll, config, &mut state.vote_results);
                    }
                }

                state
                    .polls
                    .retain(|_, v| v.data.expiry_height > height as u64);
                info!("open polls after pruning {}", state.polls.len());

                match blocks::process_block(&block, &state.chain_params, height) {
                    Ok(data) => {
                        let mut responses = Vec::new();
                        if !data.poll_creations.is_empty() {
                            info!(
                                "Storing {} poll_creations for height {}, fetching block_results",
                                data.poll_creations.len(),
                                height
                            );
                            responses.push(ProcessingResponse::SendIoCommand(
                                IoCommand::FetchBlockResults(height, data.poll_creations),
                            ));
                        }

                        for vote in data.votes {
                            if let Some(poll) = state.polls.get_mut(&vote.poll_id) {
                                debug!("vote: {:?}", vote);
                                poll.votes.push(vote);
                            } else {
                                warn!(
                                    "Got vote on poll_id={} and we don't know about it. It's fine if this program just started (~90s)",
                                    vote.poll_id
                                );
                            }
                        }

                        if state.chain_tip > state.last_processed_height {
                            let next_height = state.last_processed_height + 1;
                            info!(
                                "Still behind chain tip, immediately fetching block {}",
                                next_height
                            );
                            responses.push(ProcessingResponse::SendIoCommand(
                                IoCommand::FetchBlock(Height::Specific(next_height)),
                            ));
                        }

                        responses
                    }
                    Err(e) => {
                        error!("Failed to process block at height {}: {}", height, e);
                        vec![]
                    }
                }
            }
            IoResult::BlockResults(height, block_results, poll_creations) => {
                debug!(
                    "Processing {} poll_creations with block_results for height {}",
                    poll_creations.len(),
                    height
                );

                let poll_events = polls::extract_all_poll_events(&block_results);
                let tx_to_poll_ids: BTreeMap<String, u64> = poll_events
                    .iter()
                    .map(|pe| (hex::encode(&pe.tx_id), pe.poll_id))
                    .collect();

                for creation in poll_creations {
                    if let Some(poll_id) = tx_to_poll_ids.get(&creation.tx) {
                        let poll = Poll {
                            poll_id: *poll_id,
                            data: creation,
                            votes: vec![],
                        };
                        info!("Created poll at {height}; poll_id: {poll_id}");
                        debug!("Created poll at {height} = {poll:?}");
                        state.polls.insert(poll.poll_id, poll);
                    } else {
                        warn!(
                            "Got poll creation for tx_id={} but no matching event",
                            creation.tx
                        );
                    }
                }
                vec![]
            }
            IoResult::ChainParams(chain) => {
                state
                    .chain_params
                    .insert(chain.name.to_lowercase(), chain.clone());

                for broadcaster in &config.broadcaster {
                    for result_type in [
                        VoteResultType::Agreed,
                        VoteResultType::Disagreed,
                        VoteResultType::Missed,
                    ] {
                        let key = VoteResult {
                            broadcaster: broadcaster.name.clone(),
                            chain: chain.name.to_lowercase(),
                            result: result_type,
                        };
                        state.vote_results.entry(key).or_insert(0);
                    }
                }

                vec![]
            }
            IoResult::Head(height) => {
                let old_tip = state.chain_tip;
                state.chain_tip = height;

                if state.last_processed_height == 0 {
                    info!("Initializing: starting from current chain tip {}", height);
                    state.last_processed_height = height - 1;
                    vec![ProcessingResponse::SendIoCommand(IoCommand::FetchBlock(
                        Height::Specific(height),
                    ))]
                } else if state.chain_tip > state.last_processed_height
                    && old_tip == state.last_processed_height
                {
                    let next_height = state.last_processed_height + 1;
                    debug!(
                        "Chain advanced while caught up, fetching block {}",
                        next_height
                    );
                    vec![ProcessingResponse::SendIoCommand(IoCommand::FetchBlock(
                        Height::Specific(next_height),
                    ))]
                } else {
                    info!("Chain tip updated to {}", height);
                    vec![]
                }
            }
        },
        ProcessingMessage::QueryMetrics(response_tx) => {
            let snapshot = MetricsSnapshot {
                chain_height: state.chain_height,
                fetch_error_count: state.fetch_error_count,
                broadcaster_votes: state.vote_results.clone(),
                last_processed_height: state.last_processed_height,
            };
            vec![ProcessingResponse::SendMetricsSnapshot(
                response_tx,
                snapshot,
            )]
        }
        ProcessingMessage::Shutdown => vec![ProcessingResponse::Shutdown],
    }
}

pub fn process_single_io_command(cmd: IoCommand, rpc_url: &str, lcd_url: &str) -> Vec<IoResponse> {
    match cmd {
        IoCommand::FetchBlock(height) => match get_block(rpc_url, height) {
            Ok((block, height)) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::Block(height, block),
            ))],
            Err(e) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::FetchError(height, e.to_string()),
            ))],
        },
        IoCommand::FetchBlockResults(height, pc) => match get_block_results(lcd_url, height) {
            Ok(block_results) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::BlockResults(height, block_results, pc),
            ))],
            Err(e) => {
                error!("Failed to fetch block results for height {}: {}", height, e);
                vec![]
            }
        },
        IoCommand::FetchChainList => match get_chain_list(rpc_url) {
            Ok(chains) => {
                info!("fetched chain list: {} chains", chains.len());
                vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                    IoResult::ChainList(chains),
                ))]
            }
            Err(e) => {
                error!("Failed to fetch chain list: {}", e);
                vec![]
            }
        },
        IoCommand::FetchHead => match get_head(rpc_url) {
            Ok(height) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::Head(height),
            ))],
            Err(e) => {
                error!("Failed to fetch chain head: {}", e);
                vec![]
            }
        },
        IoCommand::Shutdown => vec![IoResponse::Shutdown],
        IoCommand::FetchChainParams(chain) => match get_chain_params(rpc_url, &chain) {
            Ok(params) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::ChainParams(params.into()),
            ))],
            Err(e) => {
                error!("Failed to fetch chain params for {}: {}", chain, e);
                vec![]
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Broadcaster;
    use crate::polls::{PollData, PollKind, PollVote};

    fn create_test_poll(poll_id: u64, votes: Vec<PollVote>) -> Poll {
        Poll {
            poll_id,
            data: PollData {
                kind: PollKind::GatewayTx,
                chain: "Ethereum".to_string(),
                tx: "test_tx".to_string(),
                expiry_height: 1000,
            },
            votes,
        }
    }

    #[test]
    fn test_analyze_poll_completion_empty_votes() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![],
            chain_params: vec![],
        };

        let poll = create_test_poll(1, vec![]);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        assert_eq!(stats.len(), 0, "No stats should be created for empty votes");
    }

    #[test]
    fn test_analyze_poll_completion_unanimous_vote() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![
                Broadcaster {
                    name: "broadcaster1".to_string(),
                    address: "addr1".to_string(),
                },
                Broadcaster {
                    name: "broadcaster2".to_string(),
                    address: "addr2".to_string(),
                },
            ],
            chain_params: vec![],
        };

        let votes = vec![
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "addr1".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "addr2".to_string(),
                payload_hash: None,
            },
        ];

        let poll = create_test_poll(1, votes);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        let agreed1 = VoteResult {
            broadcaster: "broadcaster1".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Agreed,
        };
        let agreed2 = VoteResult {
            broadcaster: "broadcaster2".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Agreed,
        };
        let missed1 = VoteResult {
            broadcaster: "broadcaster1".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };
        let missed2 = VoteResult {
            broadcaster: "broadcaster2".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };

        assert_eq!(*stats.get(&agreed1).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&missed1).unwrap_or(&0), 0);
        assert_eq!(*stats.get(&agreed2).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&missed2).unwrap_or(&0), 0);
    }

    #[test]
    fn test_analyze_poll_completion_with_disagreement() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![
                Broadcaster {
                    name: "good_broadcaster".to_string(),
                    address: "good_addr".to_string(),
                },
                Broadcaster {
                    name: "bad_broadcaster".to_string(),
                    address: "bad_addr".to_string(),
                },
            ],
            chain_params: vec![],
        };

        let votes = vec![
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "good_addr".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "good_addr".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "wrong_tx".to_string(),
                sender_id: "bad_addr".to_string(),
                payload_hash: None,
            },
        ];

        let poll = create_test_poll(1, votes);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        let good_agreed = VoteResult {
            broadcaster: "good_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Agreed,
        };
        let bad_disagreed = VoteResult {
            broadcaster: "bad_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Disagreed,
        };
        let good_missed = VoteResult {
            broadcaster: "good_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };
        let bad_missed = VoteResult {
            broadcaster: "bad_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };

        assert_eq!(*stats.get(&good_agreed).unwrap_or(&0), 2);
        assert_eq!(*stats.get(&good_missed).unwrap_or(&0), 0);
        assert_eq!(*stats.get(&bad_disagreed).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&bad_missed).unwrap_or(&0), 0);
    }

    #[test]
    fn test_analyze_poll_completion_empty_tx_id_votes() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![
                Broadcaster {
                    name: "correct_broadcaster".to_string(),
                    address: "correct_addr".to_string(),
                },
                Broadcaster {
                    name: "empty_broadcaster".to_string(),
                    address: "empty_addr".to_string(),
                },
            ],
            chain_params: vec![],
        };

        let votes = vec![
            PollVote {
                poll_id: 1,
                chain: "fantom".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "correct_addr".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "fantom".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "correct_addr".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "fantom".to_string(),
                tx_id: "".to_string(),
                sender_id: "empty_addr".to_string(),
                payload_hash: None,
            },
        ];

        let poll = create_test_poll(1, votes);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        let correct_agreed = VoteResult {
            broadcaster: "correct_broadcaster".to_string(),
            chain: "fantom".to_string(),
            result: VoteResultType::Agreed,
        };
        let empty_disagreed = VoteResult {
            broadcaster: "empty_broadcaster".to_string(),
            chain: "fantom".to_string(),
            result: VoteResultType::Disagreed,
        };

        assert_eq!(*stats.get(&correct_agreed).unwrap_or(&0), 2);
        assert_eq!(*stats.get(&empty_disagreed).unwrap_or(&0), 1);
    }

    #[test]
    fn test_analyze_poll_completion_ignores_unknown_broadcasters() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![Broadcaster {
                name: "known_broadcaster".to_string(),
                address: "known_addr".to_string(),
            }],
            chain_params: vec![],
        };

        let votes = vec![
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "known_addr".to_string(),
                payload_hash: None,
            },
            PollVote {
                poll_id: 1,
                chain: "ethereum".to_string(),
                tx_id: "correct_tx".to_string(),
                sender_id: "unknown_addr".to_string(),
                payload_hash: None,
            },
        ];

        let poll = create_test_poll(1, votes);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        assert_eq!(stats.len(), 1, "Only known broadcaster should be tracked");
        let agreed = VoteResult {
            broadcaster: "known_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Agreed,
        };
        assert_eq!(*stats.get(&agreed).unwrap_or(&0), 1);
    }

    #[test]
    fn test_analyze_poll_completion_tracks_missed_votes() {
        let config = Config {
            rpc_url: "".to_string(),
            lcd_url: "".to_string(),
            poll_interval_seconds: 5,
            metrics_port: 9090,
            broadcaster: vec![
                Broadcaster {
                    name: "active_broadcaster".to_string(),
                    address: "active_addr".to_string(),
                },
                Broadcaster {
                    name: "inactive_broadcaster".to_string(),
                    address: "inactive_addr".to_string(),
                },
                Broadcaster {
                    name: "another_inactive".to_string(),
                    address: "another_inactive_addr".to_string(),
                },
            ],
            chain_params: vec![],
        };

        let votes = vec![PollVote {
            poll_id: 1,
            chain: "ethereum".to_string(),
            tx_id: "correct_tx".to_string(),
            sender_id: "active_addr".to_string(),
            payload_hash: None,
        }];

        let poll = create_test_poll(1, votes);
        let mut stats = BTreeMap::new();

        analyze_poll_completion(&poll, &config, &mut stats);

        let active_agreed = VoteResult {
            broadcaster: "active_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Agreed,
        };
        let inactive_missed = VoteResult {
            broadcaster: "inactive_broadcaster".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };
        let another_inactive_missed = VoteResult {
            broadcaster: "another_inactive".to_string(),
            chain: "ethereum".to_string(),
            result: VoteResultType::Missed,
        };

        assert_eq!(*stats.get(&active_agreed).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&inactive_missed).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&another_inactive_missed).unwrap_or(&0), 1);
    }
}
