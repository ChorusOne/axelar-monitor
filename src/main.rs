use crate::blocks::{Block, parse_block};
use crate::config::{ChainParams, Config};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use log::{debug, error, info, warn};

mod blocks;
mod config;
mod generated;
mod metrics;
mod polls;

use polls::{Poll, PollCreation};

#[derive(Debug, Clone, Copy)]
enum Height {
    Latest,
    Specific(u64),
}

enum IoCommand {
    FetchBlock(Height),
    FetchBlockResults(u64),
    FetchChainList,
    FetchChainParams(String),
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    Block(u64, Block),
    BlockResults(u64, polls::BlockResults),
    FetchError(Height, String),
    ChainList(Vec<String>),
    ChainParams(config::ChainParams),
}

#[derive(Debug)]
struct MetricsSnapshot {
    last_heartbeat: HashMap<String, u64>,
    chain_height: u64,
    fetch_error_count: u64,
}

enum ProcessingMessage {
    IoResult(IoResult),
    QueryMetrics(mpsc::Sender<MetricsSnapshot>),
    Shutdown,
}

#[derive(Deserialize, Debug)]
struct ChainListResponse {
    chains: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct ChainParamsResponse {
    params: ChainParamsJson,
}

#[derive(Deserialize, Debug)]
struct ChainParamsJson {
    chain: String,
    revote_locking_period: String,
    voting_grace_period: String,
}

impl Into<ChainParams> for ChainParamsJson {
    fn into(self) -> ChainParams {
        ChainParams {
            name: self.chain,
            revote_locking_period: self.revote_locking_period.parse().unwrap(),
            voting_grace_period: self.voting_grace_period.parse().unwrap(),
        }
    }
}

fn get_block(base_url: &str, height: Height) -> Result<(Block, u64), Box<dyn std::error::Error>> {
    let height_str = match height {
        Height::Latest => "latest".into(),
        Height::Specific(n) => n.to_string(),
    };
    let url = format!(
        "{}/cosmos/base/tendermint/v1beta1/blocks/{}",
        base_url, height_str
    );

    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let b = parse_block(&body)?;
    let h = b.header.height.parse()?;
    Ok((b, h))
}

fn get_chain_list(base_url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let url = format!("{}/axelar/evm/v1beta1/chains", base_url);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: ChainListResponse = serde_json::from_str(&body)?;
    Ok(parsed.chains)
}

fn get_chain_params(
    base_url: &str,
    chain: &str,
) -> Result<ChainParamsJson, Box<dyn std::error::Error>> {
    let url = format!("{}/axelar/evm/v1beta1/params/{}", base_url, chain);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: ChainParamsResponse = serde_json::from_str(&body)?;
    Ok(parsed.params)
}

enum IoResponse {
    SendMessage(ProcessingMessage),
    Shutdown,
}

fn process_single_io_command(
    cmd: IoCommand,
    rpc_url: &str,
    lcd_url: &str,
) -> Vec<IoResponse> {
    match cmd {
        IoCommand::FetchBlock(height) => match get_block(rpc_url, height) {
            Ok((block, height)) => vec![IoResponse::SendMessage(
                ProcessingMessage::IoResult(IoResult::Block(height, block)),
            )],
            Err(e) => vec![IoResponse::SendMessage(
                ProcessingMessage::IoResult(IoResult::FetchError(height, e.to_string())),
            )],
        },
        IoCommand::FetchBlockResults(height) => {
            match polls::get_block_results(lcd_url, height) {
                Ok(block_results) => vec![IoResponse::SendMessage(
                    ProcessingMessage::IoResult(IoResult::BlockResults(height, block_results)),
                )],
                Err(e) => {
                    error!("Failed to fetch block results for height {}: {}", height, e);
                    vec![]
                }
            }
        }
        IoCommand::FetchChainList => match get_chain_list(rpc_url) {
            Ok(chains) => {
                info!("fetched chain list: {} chains", chains.len());
                vec![IoResponse::SendMessage(
                    ProcessingMessage::IoResult(IoResult::ChainList(chains)),
                )]
            }
            Err(e) => {
                error!("Failed to fetch chain list: {}", e);
                vec![]
            }
        },
        IoCommand::Shutdown => vec![IoResponse::Shutdown],
        IoCommand::FetchChainParams(chain) => match get_chain_params(rpc_url, &chain) {
            Ok(params) => vec![IoResponse::SendMessage(
                ProcessingMessage::IoResult(IoResult::ChainParams(params.into())),
            )],
            Err(e) => {
                error!("Failed to fetch params for chain {}: {}", &chain, e);
                vec![]
            }
        },
    }
}

fn io_thread_loop(
    rpc_url: String,
    lcd_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<ProcessingMessage>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(cmd) => {
                let responses = process_single_io_command(cmd, &rpc_url, &lcd_url);
                let mut should_shutdown = false;

                for response in responses {
                    match response {
                        IoResponse::SendMessage(msg) => {
                            msg_tx.send(msg).unwrap();
                        }
                        IoResponse::Shutdown => {
                            msg_tx.send(ProcessingMessage::Shutdown).unwrap();
                            should_shutdown = true;
                        }
                    }
                }

                if should_shutdown {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    info!("exiting io thread loop");
}

struct ProcessingState {
    chain_height: u64,
    fetch_error_count: u64,
    last_heartbeat: HashMap<String, u64>,
    chain_params: HashMap<String, ChainParams>,
    polls: HashMap<u64, Poll>,
    pending_poll_creations: HashMap<u64, Vec<PollCreation>>,
}

impl ProcessingState {
    fn new(config: &Config) -> Self {
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
        }
    }
}

enum ProcessingResponse {
    SendIoCommand(IoCommand),
    SendMetricsSnapshot(mpsc::Sender<MetricsSnapshot>, MetricsSnapshot),
    Shutdown,
}

fn process_single_message(
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
                            info!(
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
                                info!("vote: {:?}", vote);
                                poll.votes.push(vote);
                            } else {
                                warn!(
                                    "Got vote on poll_id={} and we don't know about it. It's fine if this program just started (~90s)",
                                    vote.poll_id
                                );
                            }
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
                    info!(
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
                state
                    .chain_params
                    .insert(chain.name.to_lowercase(), chain);
                vec![]
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

fn processing_loop(
    msg_rx: mpsc::Receiver<ProcessingMessage>,
    cmd_tx: mpsc::Sender<IoCommand>,
    config: Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = ProcessingState::new(&config);

    loop {
        match msg_rx.recv()? {
            msg => {
                let responses = process_single_message(msg, &mut state, &config);
                let mut should_shutdown = false;

                for response in responses {
                    match response {
                        ProcessingResponse::SendIoCommand(cmd) => {
                            cmd_tx.send(cmd).unwrap();
                        }
                        ProcessingResponse::SendMetricsSnapshot(response_tx, snapshot) => {
                            let _ = response_tx.send(snapshot);
                        }
                        ProcessingResponse::Shutdown => {
                            should_shutdown = true;
                        }
                    }
                }

                if should_shutdown {
                    break;
                }
            }
        }
    }
    info!("processing loop done");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let config = Config::load("config.toml")?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<IoCommand>();
    let (msg_tx, msg_rx) = mpsc::channel::<ProcessingMessage>();

    let rpc_url = config.rpc_url.clone();
    let lcd_url = config.lcd_url.clone();
    let poll_interval = config.poll_interval_seconds;
    let metrics_port = config.metrics_port;

    let msg_tx_io = msg_tx.clone();
    thread::spawn(move || {
        io_thread_loop(rpc_url, lcd_url, cmd_rx, msg_tx_io);
    });

    let feeder_tx = cmd_tx.clone();

    cmd_tx.send(IoCommand::FetchChainList).unwrap();

    let args: Vec<String> = std::env::args().collect();
    let single_block = if args.len() > 1 {
        args[1].parse::<u64>().ok()
    } else {
        None
    };

    if let Some(height) = single_block {
        thread::spawn(move || {
            feeder_tx
                .send(IoCommand::FetchBlock(Height::Specific(height)))
                .unwrap();
            // wait for block_result data to be fetched from rpc
            std::thread::sleep(Duration::from_secs(2));
            feeder_tx
                .send(IoCommand::FetchBlock(Height::Specific(height + 1)))
                .unwrap();
            // FetchBlockResults -- FIXME -- Shutdown should set a flag and only quit on empty
            std::thread::sleep(Duration::from_secs(1));
            feeder_tx.send(IoCommand::Shutdown).unwrap();
        });
    } else {
        thread::spawn(move || {
            loop {
                feeder_tx
                    .send(IoCommand::FetchBlock(Height::Latest))
                    .unwrap();
                thread::sleep(Duration::from_secs(poll_interval));
            }
        });
        let msg_tx_metrics = msg_tx.clone();
        thread::spawn(move || {
            metrics::metrics_server_loop(msg_tx_metrics, metrics_port);
        });
    }

    processing_loop(msg_rx, cmd_tx, config)
}
