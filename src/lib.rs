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

#[derive(Debug)]
pub struct MetricsSnapshot {
    pub last_heartbeat: HashMap<String, u64>,
    pub chain_height: u64,
    pub fetch_error_count: u64,
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

        ProcessingState {
            chain_height: 0,
            fetch_error_count: 0,
            last_heartbeat,
            chain_params,
            polls: HashMap::new(),
            pending_poll_creations: HashMap::new(),
            chain_tip: 0,
            last_processed_height: 0,
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
                info!("at height {}", state.chain_height);
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
                            info!("Created poll at {height} = {poll:?}");
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
                state.chain_params.insert(chain.name.to_lowercase(), chain);
                vec![]
            }
            IoResult::Head(height) => {
                state.chain_tip = height;
                debug!("Chain tip is at height {}", height);

                if state.last_processed_height == 0 {
                    info!("Initializing: starting from current chain tip {}", height);
                    state.last_processed_height = height - 1;
                }

                if state.chain_tip > state.last_processed_height {
                    let next_height = state.last_processed_height + 1;
                    if state.chain_tip > state.last_processed_height + 1 {
                        info!("Chain tip is at {next_height}, will start to catch up now");
                    }
                    vec![ProcessingResponse::SendIoCommand(IoCommand::FetchBlock(
                        Height::Specific(next_height),
                    ))]
                } else {
                    debug!("Caught up to chain tip");
                    vec![]
                }
            }
        },
        ProcessingMessage::QueryMetrics(response_tx) => {
            let snapshot = MetricsSnapshot {
                last_heartbeat: state.last_heartbeat.clone(),
                chain_height: state.chain_height,
                fetch_error_count: state.fetch_error_count,
            };
            vec![ProcessingResponse::SendMetricsSnapshot(
                response_tx,
                snapshot,
            )]
        }
        ProcessingMessage::Shutdown => vec![ProcessingResponse::Shutdown],
    }
}
