use crate::blocks::{Block, RawPollRequests, parse_block};
use crate::config::{ChainParams, Config};
use crate::polls::PollEvent;
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
    FetchPollEvents(u64, Vec<PollCreation>),
    FetchChainList,
    FetchChainParams(String),
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    Block(u64),
    Heartbeats(u64, Vec<String>),
    NewPollRequests(u64, Vec<PollRequest>),
    PollMappings(u64, Vec<Poll>),
    Votes(Vec<PollVote>),
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

#[derive(Debug)]
struct PollVote {
    poll_id: u64,
    chain: String,
    tx_id: String,
    sender_id: String,
    payload_hash: Option<String>,
}

#[derive(Debug)]
enum PollRequest {
    GatewayTx {
        tx: String,
        chain: String,
    },
    Deposit {
        tx: String,
        chain: String,
        burner_address: String,
    },
    TransferKey {
        tx: String,
        chain: String,
    },
}

impl PollRequest {
    fn tx(&self) -> &str {
        match self {
            PollRequest::GatewayTx { tx, .. } => tx,
            PollRequest::Deposit { tx, .. } => tx,
            PollRequest::TransferKey { tx, .. } => tx,
        }
    }

    fn chain(&self) -> &str {
        match self {
            PollRequest::GatewayTx { chain, .. } => chain,
            PollRequest::Deposit { chain, .. } => chain,
            PollRequest::TransferKey { chain, .. } => chain,
        }
    }
}

#[derive(Debug)]
enum PollType {
    GatewayTx {
        chain: String,
        tx: String,
    },
    Deposit {
        chain: String,
        tx: String,
        burner_address: String,
        // TODO: add asset field if needed (extract from ConfirmDepositStarted event)
    },
    TransferKey {
        chain: String,
        tx: String,
    },
}

#[derive(Debug)]
enum PollCreation {
    GatewayTx {
        tx: String,
        expiry_height: u64,
        chain: String,
    },
    Deposit {
        tx: String,
        expiry_height: u64,
        chain: String,
        burner_address: String,
    },
    TransferKey {
        tx: String,
        expiry_height: u64,
        chain: String,
    },
}

impl PollCreation {
    fn tx(&self) -> &str {
        match self {
            PollCreation::GatewayTx { tx, .. } => tx,
            PollCreation::Deposit { tx, .. } => tx,
            PollCreation::TransferKey { tx, .. } => tx,
        }
    }

    fn expiry_height(&self) -> u64 {
        match self {
            PollCreation::GatewayTx { expiry_height, .. } => *expiry_height,
            PollCreation::Deposit { expiry_height, .. } => *expiry_height,
            PollCreation::TransferKey { expiry_height, .. } => *expiry_height,
        }
    }

    fn chain(&self) -> &str {
        match self {
            PollCreation::GatewayTx { chain, .. } => chain,
            PollCreation::Deposit { chain, .. } => chain,
            PollCreation::TransferKey { chain, .. } => chain,
        }
    }
}

#[derive(Debug)]
struct Poll {
    poll_id: u64,
    poll_type: PollType,
    votes: Vec<PollVote>,
    expiry_height: u64,
}

impl Poll {
    fn tx(&self) -> &str {
        match &self.poll_type {
            PollType::GatewayTx { tx, .. } => tx,
            PollType::Deposit { tx, .. } => tx,
            PollType::TransferKey { tx, .. } => tx,
        }
    }

    fn chain(&self) -> &str {
        match &self.poll_type {
            PollType::GatewayTx { chain, .. } => chain,
            PollType::Deposit { chain, .. } => chain,
            PollType::TransferKey { chain, .. } => chain,
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

                            let reqs = blocks::extract_raw_poll_requests(&txs);

                            let poll_requests: Vec<PollRequest> = reqs
                                .iter()
                                .map(|r| match r {
                                    RawPollRequests::GatewayTx(g) => vec![PollRequest::GatewayTx {
                                        tx: hex::encode(&g.tx_id),
                                        chain: g.chain.clone(),
                                    }],
                                    RawPollRequests::GatewayTxs(g) => g
                                        .tx_ids
                                        .iter()
                                        .map(|tx_id| PollRequest::GatewayTx {
                                            tx: hex::encode(&tx_id),
                                            chain: g.chain.clone(),
                                        })
                                        .collect(),
                                    RawPollRequests::Deposit(d) => vec![PollRequest::Deposit {
                                        tx: hex::encode(&d.tx_id),
                                        chain: d.chain.clone(),
                                        burner_address: hex::encode(&d.burner_address),
                                    }],
                                    RawPollRequests::TransferKey(t) => {
                                        vec![PollRequest::TransferKey {
                                            tx: hex::encode(&t.tx_id),
                                            chain: t.chain.clone(),
                                        }]
                                    }
                                })
                                .flatten()
                                .collect();

                            if !poll_requests.is_empty() {
                                msg_tx
                                    .send(ProcessingMessage::IoResult(IoResult::NewPollRequests(
                                        height,
                                        poll_requests,
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
                        .send(ProcessingMessage::IoResult(IoResult::ChainParams(
                            params.into(),
                        )))
                        .unwrap();
                }
                Err(e) => {
                    error!("Failed to fetch params for chain {}: {}", &chain, e);
                }
            },
            // merge poll-creation txs with block_results
            // into a complete poll (with poll_id)
            Ok(IoCommand::FetchPollEvents(height, poll_creations)) => {
                match polls::get_block_results(&lcd_url, height) {
                    Ok(block_results) => {
                        let mut complete_polls = Vec::new();

                        // Extract poll_mappings for batch GatewayTxs events
                        let poll_mappings =
                            polls::extract_poll_mappings_from_events(&block_results);

                        for poll_mapping in poll_mappings {
                            let tx_id_hex = hex::encode(&poll_mapping.tx_id);

                            if let Some(creation) =
                                poll_creations.iter().find(|pc| pc.tx() == tx_id_hex)
                            {
                                if let PollCreation::GatewayTx { chain, .. } = creation {
                                    complete_polls.push(Poll {
                                        poll_id: poll_mapping.poll_id,
                                        poll_type: PollType::GatewayTx {
                                            chain: chain.clone(),
                                            tx: tx_id_hex.clone(),
                                        },
                                        votes: vec![],
                                        expiry_height: creation.expiry_height(),
                                    });
                                }
                            } else {
                                warn!(
                                    "Got poll_mapping for tx_id={} but no matching PollCreation",
                                    tx_id_hex
                                );
                            }
                        }

                        let poll_events = polls::extract_all_poll_events(&block_results);
                        let tx_to_poll_ids: HashMap<String, u64> = poll_events
                            .iter()
                            .map(|pe| (pe.tx(), pe.poll_id()))
                            .collect();

                        for creation in poll_creations {
                            let tx_id = creation.tx();
                            if let Some(poll_id) = tx_to_poll_ids.get(tx_id) {
                                match &creation {
                                    PollCreation::GatewayTx { chain, .. } => {
                                        complete_polls.push(Poll {
                                            poll_id: *poll_id,
                                            poll_type: PollType::GatewayTx {
                                                chain: chain.clone(),
                                                tx: tx_id.into(),
                                            },
                                            votes: vec![],
                                            expiry_height: creation.expiry_height(),
                                        });
                                    }
                                    PollCreation::Deposit {
                                        chain,
                                        burner_address,
                                        ..
                                    } => {
                                        complete_polls.push(Poll {
                                            poll_id: *poll_id,
                                            poll_type: PollType::Deposit {
                                                chain: chain.clone(),
                                                tx: tx_id.into(),
                                                burner_address: burner_address.clone(),
                                            },
                                            votes: vec![],
                                            expiry_height: creation.expiry_height(),
                                        });
                                        info!("Pushing Deposit poll with id {}", poll_id);
                                    }
                                    PollCreation::TransferKey { chain, .. } => {
                                        complete_polls.push(Poll {
                                            poll_id: *poll_id,
                                            poll_type: PollType::TransferKey {
                                                chain: chain.clone(),
                                                tx: tx_id.into(),
                                            },
                                            votes: vec![],
                                            expiry_height: creation.expiry_height(),
                                        });
                                    }
                                }
                            } else {
                                warn!(
                                    "Got poll event for tx_id={} but no matching PollCreation",
                                    tx_id
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

    let mut chain_params: HashMap<String, ChainParams> =
        HashMap::with_capacity(config.chain_params.len());
    let mut polls: HashMap<u64, Poll> = HashMap::new();

    for chain_param in config.chain_params {
        chain_params.insert(chain_param.name.to_lowercase(), chain_param);
    }

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
                        if !chain_params.contains_key(&chain) {
                            cmd_tx.send(IoCommand::FetchChainParams(chain)).unwrap();
                        }
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
                // merge poll data (tx) with chain params
                IoResult::NewPollRequests(height, poll_requests) => {
                    info!(
                        "Processing {} poll_requests for height {}",
                        poll_requests.len(),
                        height
                    );

                    let mut poll_creations = Vec::new();

                    // Convert PollRequest to PollCreation by calculating expiry heights
                    for request in poll_requests {
                        info!(
                            "Processing poll request: tx={}, chain={}",
                            request.tx(),
                            request.chain()
                        );
                        let chain = request.chain();
                        if let Some(params) = chain_params.get(&chain.to_lowercase()) {
                            let revote_period = params.revote_locking_period as u64;
                            let expiry_height = height as u64 + revote_period;

                            let pc = match request {
                                PollRequest::GatewayTx { chain, tx } => PollCreation::GatewayTx {
                                    tx,
                                    chain,
                                    expiry_height,
                                },
                                PollRequest::Deposit {
                                    chain,
                                    tx,
                                    burner_address,
                                } => PollCreation::Deposit {
                                    tx,
                                    chain,
                                    burner_address,
                                    expiry_height,
                                },
                                PollRequest::TransferKey { chain, tx } => {
                                    PollCreation::TransferKey {
                                        tx,
                                        chain,
                                        expiry_height,
                                    }
                                }
                            };
                            info!("Poll complete {:?}", pc);
                            poll_creations.push(pc);
                        } else {
                            error!(
                                "Chain params not available for chain: {}. Skipping poll creation.",
                                chain
                            );
                        }
                    }

                    if !poll_creations.is_empty() {
                        debug!(
                            "Sending FetchPollEvents for height {} with {} poll_creations",
                            height,
                            poll_creations.len()
                        );
                        cmd_tx
                            .send(IoCommand::FetchPollEvents(height, poll_creations))
                            .unwrap();
                    }
                }
                IoResult::PollMappings(height, complete_polls) => {
                    for poll in complete_polls {
                        info!("Received poll at {height} = {poll:?}");
                        polls.insert(poll.poll_id, poll);
                    }
                }
                IoResult::Votes(votes) => {
                    for vote in votes {
                        let poll = polls.get_mut(&vote.poll_id);

                        if let Some(p) = poll {
                            info!("vote: {:?}", vote);
                            p.votes.push(vote);
                        } else {
                            warn!(
                                "Got vote on poll_id={} and we don't know about it. It's fine if this program just started (~90s)",
                                vote.poll_id
                            );
                        }
                    }
                }
                IoResult::ChainParams(chain) => {
                    chain_params.insert(chain.name.to_lowercase(), chain);
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
