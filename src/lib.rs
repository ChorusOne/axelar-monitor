pub mod blocks;
pub mod config;
mod generated;
pub mod polls;

pub use config::Config;

use blocks::Block;
use config::ChainParams;
use log::{debug, error, info, warn};
use polls::{Poll, PollCreation};
use std::collections::HashMap;
use std::sync::mpsc;

#[derive(Debug, Clone, Copy)]
pub enum Height {
    Latest,
    Specific(u64),
}

pub enum IoCommand {
    FetchBlock(Height),
    FetchBlockResults(u64),
    FetchChainList,
    FetchChainParams(String),
    FetchHead,
    #[allow(dead_code)]
    Shutdown,
}

pub enum IoResult {
    Block(u64, Block),
    BlockResults(u64, polls::BlockResults),
    FetchError(Height, String),
    ChainList(Vec<String>),
    ChainParams(config::ChainParams),
    Head(u64),
}

#[derive(Debug, Clone)]
pub struct BroadcasterStats {
    pub total_votes: u64,
    pub disagreed_with_majority: u64,
}

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub last_heartbeat: HashMap<String, u64>,
    pub chain_height: u64,
    pub fetch_error_count: u64,
    pub broadcaster_stats: HashMap<(String, String), BroadcasterStats>,
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
    pub last_heartbeat: HashMap<String, u64>,
    pub chain_params: HashMap<String, ChainParams>,
    pub polls: HashMap<u64, Poll>,
    pub pending_poll_creations: HashMap<u64, Vec<PollCreation>>,
    pub chain_tip: u64,
    pub last_processed_height: u64,
    pub broadcaster_stats: HashMap<(String, String), BroadcasterStats>,
}

impl ProcessingState {
    pub fn new(config: &Config) -> Self {
        let last_heartbeat: HashMap<String, u64> = config
            .broadcaster
            .iter()
            .map(|bc| (bc.name.clone(), 0))
            .collect();

        let mut chain_params: HashMap<String, ChainParams> =
            HashMap::with_capacity(config.chain_params.len());
        for chain_param in &config.chain_params {
            chain_params.insert(chain_param.name.to_lowercase(), chain_param.clone());
        }

        let mut broadcaster_stats = HashMap::new();
        for broadcaster in &config.broadcaster {
            for chain_param in &config.chain_params {
                let key = (broadcaster.name.clone(), chain_param.name.to_lowercase());
                broadcaster_stats.insert(
                    key,
                    BroadcasterStats {
                        total_votes: 0,
                        disagreed_with_majority: 0,
                    },
                );
            }
        }

        ProcessingState {
            chain_height: 0,
            fetch_error_count: 0,
            last_heartbeat,
            chain_params,
            polls: HashMap::new(),
            pending_poll_creations: HashMap::new(),
            chain_tip: 0,
            last_processed_height: 0,
            broadcaster_stats,
        }
    }
}

fn analyze_poll_completion(
    poll: &Poll,
    config: &Config,
    broadcaster_stats: &mut HashMap<(String, String), BroadcasterStats>,
) {
    if poll.votes.is_empty() {
        return;
    }

    let mut tx_id_counts: HashMap<&str, u64> = HashMap::new();
    for vote in &poll.votes {
        *tx_id_counts.entry(&vote.tx_id).or_insert(0) += 1;
    }

    let majority_tx_id = tx_id_counts
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(tx_id, _)| *tx_id);

    if let Some(majority) = majority_tx_id {
        let sender_to_broadcaster: HashMap<String, &str> = config
            .broadcaster
            .iter()
            .map(|bc| (bc.address.clone(), bc.name.as_str()))
            .collect();

        for vote in &poll.votes {
            if let Some(broadcaster_name) = sender_to_broadcaster.get(&vote.sender_id) {
                let key = (broadcaster_name.to_string(), vote.chain.to_lowercase());
                let stats = broadcaster_stats.entry(key).or_insert(BroadcasterStats {
                    total_votes: 0,
                    disagreed_with_majority: 0,
                });

                stats.total_votes += 1;
                if vote.tx_id != majority {
                    stats.disagreed_with_majority += 1;
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
                debug!(
                    "got block height is {}, chain height is {}",
                    height, state.chain_height
                );

                let expiring_poll_ids: Vec<_> = state
                    .polls
                    .iter()
                    .filter(|(_, poll)| poll.expiry_height <= height as u64)
                    .map(|(id, _)| *id)
                    .collect();

                for poll_id in expiring_poll_ids {
                    if let Some(poll) = state.polls.get(&poll_id) {
                        analyze_poll_completion(poll, config, &mut state.broadcaster_stats);
                    }
                }

                state.polls.retain(|_, v| v.expiry_height > height as u64);
                debug!("open polls after pruning {}", state.polls.len());

                match blocks::process_block(&block, &state.chain_params, height) {
                    Ok(data) => {
                        for addr in data.heartbeat_addrs {
                            for bc in &config.broadcaster {
                                if bc.address == addr {
                                    info!("Height {}: {} heartbeat detected", height, bc.name);
                                    state.last_heartbeat.insert(bc.name.clone(), height);
                                    break;
                                }
                            }
                        }

                        let mut responses = Vec::new();
                        if !data.poll_creations.is_empty() {
                            debug!(
                                "Storing {} poll_creations for height {}, fetching block_results",
                                data.poll_creations.len(),
                                height
                            );
                            state
                                .pending_poll_creations
                                .insert(height, data.poll_creations);
                            responses.push(ProcessingResponse::SendIoCommand(
                                IoCommand::FetchBlockResults(height),
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
                            debug!(
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
            IoResult::BlockResults(height, block_results) => {
                if let Some(poll_creations) = state.pending_poll_creations.remove(&height) {
                    debug!(
                        "Processing {} poll_creations with block_results for height {}",
                        poll_creations.len(),
                        height
                    );

                    let poll_events = polls::extract_all_poll_events(&block_results);
                    let tx_to_poll_ids: HashMap<String, u64> = poll_events
                        .iter()
                        .map(|pe| (pe.tx(), pe.poll_id()))
                        .collect();

                    for creation in poll_creations {
                        let tx_id = creation.tx();
                        if let Some(poll_id) = tx_to_poll_ids.get(tx_id) {
                            let poll = creation.into_poll(*poll_id, tx_id.into());
                            info!("Created poll at {height}; poll_id: {poll_id}");
                            debug!("Created poll at {height} = {poll:?}");
                            state.polls.insert(poll.poll_id, poll);
                        } else {
                            warn!(
                                "Got poll creation for tx_id={} but no matching event",
                                tx_id
                            );
                        }
                    }
                } else {
                    debug!(
                        "Received block_results for height {} but no pending poll creations",
                        height
                    );
                }
                vec![]
            }
            IoResult::ChainParams(chain) => {
                state
                    .chain_params
                    .insert(chain.name.to_lowercase(), chain.clone());

                for broadcaster in &config.broadcaster {
                    let key = (broadcaster.name.clone(), chain.name.to_lowercase());
                    state
                        .broadcaster_stats
                        .entry(key)
                        .or_insert(BroadcasterStats {
                            total_votes: 0,
                            disagreed_with_majority: 0,
                        });
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
                    debug!("Chain tip updated to {}", height);
                    vec![]
                }
            }
        },
        ProcessingMessage::QueryMetrics(response_tx) => {
            let snapshot = MetricsSnapshot {
                last_heartbeat: state.last_heartbeat.clone(),
                chain_height: state.chain_height,
                fetch_error_count: state.fetch_error_count,
                broadcaster_stats: state.broadcaster_stats.clone(),
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
