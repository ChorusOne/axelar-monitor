use crate::blocks::{Block, parse_block};
use crate::config::{Broadcaster, Config};
use cosmos_sdk_proto::cosmos::tx::v1beta1::TxBody;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod blocks;
mod config;
mod generated;
mod metrics;

#[derive(Debug, Clone, Copy)]
enum Height {
    Latest,
    Specific(u64),
}

enum IoCommand {
    FetchBlock(Height),
    FetchChainParams(String),
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    BlockFetched(u64, Block),
    FetchError(Height, String),
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
    tx_id: String,
    sender_id: String,
    payload_hash: Option<String>,
}

#[derive(Debug)]
struct Poll {
    votes: Vec<PollVote>,
    expiry_height: u64,
    chain: String,
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

fn heartbeat_blocks_for(height: u64) -> Vec<u64> {
    let cycle_base = (height / 50) * 50;
    vec![cycle_base, cycle_base + 1, cycle_base + 2]
}

fn io_thread_loop(
    rpc_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<ProcessingMessage>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(IoCommand::FetchBlock(height)) => match get_block(&rpc_url, height) {
                Ok((block, height)) => {
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::BlockFetched(
                            height, block,
                        )))
                        .unwrap();
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
            Ok(IoCommand::Shutdown) => {
                msg_tx.send(ProcessingMessage::Shutdown).unwrap();
                break;
            }
            Ok(IoCommand::FetchChainParams(chain)) => match get_chain_params(&rpc_url, &chain) {
                Ok(params) => {
                    println!("fetched chain params for {chain}");
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::ChainParams(params)))
                        .unwrap();
                }
                Err(e) => {
                    eprintln!("Failed to fetch params for chain {}: {}", &chain, e);
                }
            },
            Err(_) => break,
        }
    }
    println!("exiting io thread loop");
}

fn heartbeats_from_our_broadcasters<'a>(
    txs: &[TxBody],
    config: &'a Config,
) -> Vec<&'a Broadcaster> {
    let mut ret = Vec::with_capacity(config.broadcaster.len());

    for txbody in txs {
        let heartbeat_messages = blocks::extract_heartbeat_requests(&txbody);
        for hb in heartbeat_messages {
            let encoded_addr = hex::encode(&hb.sender);
            for bc in &config.broadcaster {
                if bc.address == encoded_addr {
                    ret.push(bc);
                    break;
                }
            }
        }
    }
    ret
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

    let mut chain_params_cache: HashMap<String, ChainParams> = HashMap::new();
    let mut polls: HashMap<String, Poll> = HashMap::new();

    loop {
        match msg_rx.recv()? {
            ProcessingMessage::IoResult(result) => match result {
                IoResult::FetchError(h, e) => {
                    fetch_error_count += 1;
                    eprintln!("Error fetching at height {:?}: {}", h, e);
                }
                // TODO should split here to Heartbeat / NewPoll / Vote
                IoResult::BlockFetched(height, block) => {
                    chain_height = std::cmp::max(chain_height, height);
                    println!("\nChecking {:?}", height);
                    let txs = blocks::get_txs(&block)?;
                    for broadcaster in heartbeats_from_our_broadcasters(&txs, &config) {
                        println!("Height {}: {} heartbeat detected", height, broadcaster.name);
                        last_heartbeat.insert(broadcaster.name.clone(), height);
                    }

                    println!("checking tx");
                    let confirm_reqs = blocks::extract_confirm_gateway_txs_requests(&txs);
                    for req in &confirm_reqs {
                        println!("ConfirmGatewayTxsRequest:");
                        let revote_period = if let Some(params) = chain_params_cache.get(&req.chain)
                        {
                            params.revote_locking_period.parse::<u64>().unwrap()
                        } else {
                            cmd_tx
                                .send(IoCommand::FetchChainParams(req.chain.clone()))
                                .unwrap();
                            20 // TODO config
                        };
                        let expiry_height = height as u64 + revote_period;
                        // not sure, but it doesn't make sense to be != 1
                        assert_eq!(req.tx_ids.len(), 1);
                        for tx_id in &req.tx_ids {
                            let encoded_tx_id = hex::encode(tx_id);
                            let poll = Poll {
                                votes: vec![],
                                expiry_height,
                                chain: req.chain.clone(),
                            };
                            println!("New poll: {encoded_tx_id} {poll:?}");
                            polls.insert(encoded_tx_id, poll);
                        }
                    }

                    /*
                    for tx in &txs {
                        blocks::print_all_refund_inner_message_types(&tx);
                    }
                    */
                    for vote in blocks::get_votes_from_txs(&txs) {
                        if let Some(p) = polls.get_mut(&vote.tx_id) {
                            p.votes.push(vote);
                        }
                    }

                    polls.retain(|_, v| v.expiry_height > height as u64);
                    println!("open polls after pruning {}", polls.len());
                }
                IoResult::ChainParams(chain) => {
                    println!("got chain params {:?}", chain);
                    chain_params_cache.insert(chain.chain.clone(), chain);
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
    println!("processing loop done");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load("config.toml")?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<IoCommand>();
    let (msg_tx, msg_rx) = mpsc::channel::<ProcessingMessage>();

    let rpc_url = config.rpc_url.clone();
    let poll_interval = config.poll_interval_seconds;
    let metrics_port = config.metrics_port;

    let msg_tx_io = msg_tx.clone();
    thread::spawn(move || {
        io_thread_loop(rpc_url, cmd_rx, msg_tx_io);
    });

    let feeder_tx = cmd_tx.clone();

    let args: Vec<String> = std::env::args().collect();
    let single_block = if args.len() > 1 {
        args[1].parse::<u64>().ok()
    } else {
        None
    };

    if let Some(height) = single_block {
        feeder_tx
            .send(IoCommand::FetchBlock(Height::Specific(height)))
            .unwrap();
        feeder_tx
            .send(IoCommand::FetchBlock(Height::Specific(height + 1)))
            .unwrap();
        thread::spawn(move || {
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
