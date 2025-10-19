use crate::generated::axelar::evm::v1beta1::ConfirmGatewayTxsRequest;

use crate::blocks::{Block, parse_block};
use crate::config::Config;
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

#[derive(Debug, Clone, Copy)]
enum Height {
    Latest,
    Specific(u64),
}

enum IoCommand {
    FetchBlock(Height),
    FetchBlockResults(u64, Vec<PollCreation>),
    FetchChainList,
    FetchChainParams(String),
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    Block(u64),
    Heartbeats(u64, Vec<String>),
    NewPolls(u64, Vec<ConfirmGatewayTxsRequest>),
    PollMappings(u64, Vec<Poll>),
    Votes(Vec<PollVote>),
    FetchError(Height, String),
    ChainList(Vec<String>),
    ChainParams(ChainParams),
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
    params: ChainParams,
}

#[derive(Deserialize, Debug)]
struct ChainParams {
    chain: String,
    revote_locking_period: String,
    voting_grace_period: String,
}

#[derive(Debug)]
struct PollVote {
    poll_id: u64,
    chain: String,
    tx_id: String,
    sender_id: String,
    payload_hash: Option<String>,
}

#[derive(Debug)]
struct PollCreation {
    tx: String,
    expiry_height: u64,
    chain: String,
}

#[derive(Debug)]
struct Poll {
    poll_id: u64,
    votes: Vec<PollVote>,
    expiry_height: u64,
    chain: String,
    tx: String,
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
) -> Result<ChainParams, Box<dyn std::error::Error>> {
    let url = format!("{}/axelar/evm/v1beta1/params/{}", base_url, chain);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: ChainParamsResponse = serde_json::from_str(&body)?;
    Ok(parsed.params)
}

fn io_thread_loop(
    rpc_url: String,
    lcd_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<ProcessingMessage>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(IoCommand::FetchBlock(height)) => match get_block(&rpc_url, height) {
                Ok((block, height)) => {
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::Block(height)))
                        .unwrap();

                    match blocks::get_txs(&block) {
                        Ok(txs) => {
                            let heartbeat_addrs: Vec<String> = txs
                                .iter()
                                .flat_map(|tx| blocks::extract_heartbeat_requests(tx))
                                .map(|hb| hex::encode(&hb.sender))
                                .collect();

                            if !heartbeat_addrs.is_empty() {
                                msg_tx
                                    .send(ProcessingMessage::IoResult(IoResult::Heartbeats(
                                        height,
                                        heartbeat_addrs,
                                    )))
                                    .unwrap();
                            }

                            // TODO (ConfirmGatewayTx, ConfirmDeposit, ConfirmToken, ConfirmTransferKey)
                            let confirm_reqs = blocks::extract_confirm_gateway_txs_requests(&txs);
                            if !confirm_reqs.is_empty() {
                                msg_tx
                                    .send(ProcessingMessage::IoResult(IoResult::NewPolls(
                                        height,
                                        confirm_reqs,
                                    )))
                                    .unwrap();
                            }

                            let votes = blocks::get_votes_from_txs(&txs);
                            if !votes.is_empty() {
                                msg_tx
                                    .send(ProcessingMessage::IoResult(IoResult::Votes(votes)))
                                    .unwrap();
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse txs at height {}: {}", height, e);
                        }
                    }
                }

                Err(e) => {
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::FetchError(
                            height,
                            e.to_string(),
                        )))
                        .unwrap();
                }
            },
            Ok(IoCommand::FetchChainList) => match get_chain_list(&rpc_url) {
                Ok(chains) => {
                    info!("fetched chain list: {} chains", chains.len());
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::ChainList(chains)))
                        .unwrap();
                }
                Err(e) => {
                    error!("Failed to fetch chain list: {}", e);
                }
            },
            Ok(IoCommand::Shutdown) => {
                msg_tx.send(ProcessingMessage::Shutdown).unwrap();
                break;
            }
            Ok(IoCommand::FetchChainParams(chain)) => match get_chain_params(&rpc_url, &chain) {
                Ok(params) => {
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::ChainParams(params)))
                        .unwrap();
                }
                Err(e) => {
                    error!("Failed to fetch params for chain {}: {}", &chain, e);
                }
            },
            Ok(IoCommand::FetchBlockResults(height, poll_creations)) => {
                match polls::get_block_results(&lcd_url, height) {
                    Ok(block_results) => {
                        let poll_mappings =
                            polls::extract_poll_mappings_from_events(&block_results);

                        if !poll_mappings.is_empty() {
                            let mut complete_polls = Vec::new();

                            for poll_mapping in poll_mappings {
                                let tx_id_hex = hex::encode(&poll_mapping.tx_id);

                                if let Some(creation) =
                                    poll_creations.iter().find(|pc| pc.tx == tx_id_hex)
                                {
                                    complete_polls.push(Poll {
                                        poll_id: poll_mapping.poll_id,
                                        votes: vec![],
                                        expiry_height: creation.expiry_height,
                                        chain: creation.chain.clone(),
                                        tx: tx_id_hex,
                                    });
                                } else {
                                    warn!(
                                        "Got poll_mapping for tx_id={} but no matching PollCreation",
                                        tx_id_hex
                                    );
                                }
                            }

                            if !complete_polls.is_empty() {
                                msg_tx
                                    .send(ProcessingMessage::IoResult(IoResult::PollMappings(
                                        height,
                                        complete_polls,
                                    )))
                                    .unwrap();
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to fetch block results for height {}: {}", height, e);
                    }
                }
            }
            Err(_) => break,
        }
    }
    info!("exiting io thread loop");
}

fn processing_loop(
    msg_rx: mpsc::Receiver<ProcessingMessage>,
    cmd_tx: mpsc::Sender<IoCommand>,
    config: Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut chain_height = 0;
    let mut fetch_error_count = 0u64;
    let mut last_heartbeat: HashMap<String, u64> = config
        .broadcaster
        .iter()
        .map(|bc| (bc.name.clone(), 0))
        .collect();

    let mut chain_params: HashMap<String, ChainParams> = HashMap::new();
    let mut polls: HashMap<String, Poll> = HashMap::new();
    let mut poll_id_to_tx_id: HashMap<u64, String> = HashMap::new();

    loop {
        match msg_rx.recv()? {
            ProcessingMessage::IoResult(result) => match result {
                IoResult::FetchError(h, e) => {
                    fetch_error_count += 1;
                    error!("Error fetching at height {:?}: {}", h, e);
                }
                IoResult::ChainList(chains) => {
                    info!(
                        "Chain list received, fetching params for {} chains",
                        chains.len()
                    );
                    for chain in chains {
                        cmd_tx.send(IoCommand::FetchChainParams(chain)).unwrap();
                    }
                }
                IoResult::Block(height) => {
                    chain_height = std::cmp::max(chain_height, height);
                    info!("at height {chain_height}");
                    polls.retain(|_, v| v.expiry_height > height as u64);
                    debug!("open polls after pruning {}", polls.len());
                }
                IoResult::Heartbeats(height, addresses) => {
                    for addr in addresses {
                        for bc in &config.broadcaster {
                            if bc.address == addr {
                                info!("Height {}: {} heartbeat detected", height, bc.name);
                                last_heartbeat.insert(bc.name.clone(), height);
                                break;
                            }
                        }
                    }
                }
                IoResult::NewPolls(height, confirm_reqs) => {
                    debug!("Sending FetchBlockResults command for height {}", height);
                    let mut poll_creations: Vec<PollCreation> = Vec::new();

                    for req in &confirm_reqs {
                        info!(
                            "Height {}: ConfirmGatewayTxsRequest received for chain '{}' with {} tx(s) - this will emit ConfirmGatewayTxsStarted event",
                            height,
                            req.chain,
                            req.tx_ids.len()
                        );
                        if let Some(params) = chain_params.get(&req.chain.to_lowercase()) {
                            let revote_period =
                                params.revote_locking_period.parse::<u64>().unwrap();
                            let expiry_height = height as u64 + revote_period;
                            assert_eq!(req.tx_ids.len(), 1);
                            for tx_id in &req.tx_ids {
                                let encoded_tx_id = hex::encode(tx_id);
                                info!(
                                    "  ConfirmGatewayTxsStarted: chain={}, tx_id={}, expiry_height={}",
                                    req.chain, encoded_tx_id, expiry_height
                                );
                                let poll = PollCreation {
                                    expiry_height,
                                    chain: req.chain.clone(),
                                    tx: encoded_tx_id,
                                };
                                poll_creations.push(poll);
                            }
                        } else {
                            error!(
                                "Chain params not available for chain: {}. Skipping poll creation.",
                                req.chain
                            );
                        }
                    }
                    cmd_tx
                        .send(IoCommand::FetchBlockResults(height, poll_creations))
                        .unwrap();
                }
                IoResult::PollMappings(height, complete_polls) => {
                    for poll in complete_polls {
                        info!(
                            "Height {}: ConfirmGatewayTxsStarted event - tx_id={}, poll_id={}, chain={}, expiry_height={}",
                            height, poll.tx, poll.poll_id, poll.chain, poll.expiry_height
                        );

                        poll_id_to_tx_id.insert(poll.poll_id, poll.tx.clone());
                        polls.insert(poll.tx.clone(), poll);
                    }
                }
                IoResult::Votes(votes) => {
                    for vote in votes {
                        let poll = if !vote.tx_id.is_empty() {
                            polls.get_mut(&vote.tx_id)
                        } else if let Some(tx_id) = poll_id_to_tx_id.get(&vote.poll_id) {
                            polls.get_mut(tx_id)
                        } else {
                            None
                        };

                        if let Some(p) = poll {
                            info!("vote: {:?}", vote);
                            p.votes.push(vote);
                        } else {
                            warn!(
                                "Got vote on tx_id={} poll_id={} and we don't know about it. It's fine if this program just started (~90s)",
                                vote.tx_id, vote.poll_id
                            );
                        }
                    }
                }
                IoResult::ChainParams(chain) => {
                    info!("got chain params {:?}", chain);
                    chain_params.insert(chain.chain.to_lowercase(), chain);
                }
            },
            ProcessingMessage::QueryMetrics(response_tx) => {
                let snapshot = MetricsSnapshot {
                    last_heartbeat: last_heartbeat.clone(),
                    chain_height,
                    fetch_error_count,
                };
                let _ = response_tx.send(snapshot);
            }
            ProcessingMessage::Shutdown => break,
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
            std::thread::sleep(Duration::from_secs(1));
            feeder_tx
                .send(IoCommand::FetchBlock(Height::Specific(height)))
                .unwrap();
            std::thread::sleep(Duration::from_secs(1));
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
