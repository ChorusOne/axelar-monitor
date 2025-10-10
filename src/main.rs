use crate::blocks::{Block, parse_block};
use crate::config::{Broadcaster, Config};
use crate::generated::axelar::evm::v1beta1::event::Event;
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

#[derive(Debug)]
enum Height {
    Latest,
    Specific(u32),
}

enum IoCommand {
    FetchLatest,
    FetchBlock(u32),
    FetchChainParams(String),
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    LatestFetched(u32, Block),
    BlockFetched(u32, Block),
    FetchError(Height, String),
    ChainParams(ChainParams),
}

#[derive(Debug)]
struct MetricsSnapshot {
    last_heartbeat: HashMap<String, u32>,
    last_visited_height: u32,
    chain_height: u32,
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
    sender_id: String,
    payload_hash: Option<String>,
}

#[derive(Debug)]
struct Poll {
    votes: Vec<PollVote>,
    expiry_height: u64,
    chain: String,
}

fn get_block(base_url: &str, height: Height) -> Result<Block, Box<dyn std::error::Error>> {
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
    parse_block(&body)
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

fn heartbeat_blocks_for(height: u32) -> Vec<u32> {
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
            Ok(IoCommand::FetchLatest) => match get_block(&rpc_url, Height::Latest) {
                Ok(block) => {
                    let height = block.header.height.parse::<u32>().unwrap();
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::LatestFetched(
                            height, block,
                        )))
                        .unwrap();
                }
                Err(e) => {
                    msg_tx
                        .send(ProcessingMessage::IoResult(IoResult::FetchError(
                            Height::Latest,
                            e.to_string(),
                        )))
                        .unwrap();
                }
            },
            Ok(IoCommand::FetchBlock(height)) => {
                match get_block(&rpc_url, Height::Specific(height)) {
                    Ok(block) => {
                        msg_tx
                            .send(ProcessingMessage::IoResult(IoResult::BlockFetched(
                                height, block,
                            )))
                            .unwrap();
                    }
                    Err(e) => {
                        msg_tx
                            .send(ProcessingMessage::IoResult(IoResult::FetchError(
                                Height::Specific(height),
                                e.to_string(),
                            )))
                            .unwrap();
                    }
                }
            }
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
    let mut last_visited_height = 0;
    let mut chain_height = 0;
    let mut fetch_error_count = 0u64;
    let mut last_heartbeat: HashMap<String, u32> = config
        .broadcaster
        .iter()
        .map(|bc| (bc.name.clone(), 0))
        .collect();

    let mut chain_params_cache: HashMap<String, ChainParams> = HashMap::new();
    let mut polls: HashMap<String, Poll> = HashMap::new();

    loop {
        match msg_rx.recv()? {
            ProcessingMessage::IoResult(result) => match result {
                IoResult::LatestFetched(height, _block) => {
                    chain_height = height;
                    let heights_to_check: Vec<u32> = heartbeat_blocks_for(height)
                        .iter()
                        .copied()
                        .filter(|b| *b > last_visited_height)
                        .filter(|b| *b <= height)
                        .collect();
                    for height in &heights_to_check {
                        cmd_tx.send(IoCommand::FetchBlock(*height))?;
                    }

                    println!("Current height = {height}")
                }
                IoResult::FetchError(h, e) => {
                    fetch_error_count += 1;
                    eprintln!("Error fetching at height {:?}: {}", h, e);
                }
                // TODO should split here to Heartbeat / NewPoll / Vote
                IoResult::BlockFetched(height, block) => {
                    println!("\nChecking {:?}", height);
                    let txs = blocks::get_txs(&block)?;
                    for broadcaster in heartbeats_from_our_broadcasters(&txs, &config) {
                        println!("Height {}: {} heartbeat detected", height, broadcaster.name);
                        last_heartbeat.insert(broadcaster.name.clone(), height);
                    }
                    last_visited_height = std::cmp::max(last_visited_height, height);

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
                        assert_eq!(req.tx_ids.len(), 1); // not sure, but it doesn't make sense to
                        // be != 1
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

                    for tx in &txs {
                        // blocks::print_all_refund_inner_message_types(&tx);
                    }

                    for vote in blocks::extract_decoded_votes(&txs) {
                        let sender_id = hex::encode(&vote.sender);
                        println!("Vote:");
                        println!("  poll_id: {}", vote.poll_id);
                        println!("  voter: {}", sender_id);

                        if let Some(vote_events) = &vote.vote_events {
                            println!("  chain: {}", vote_events.chain);
                            println!("  events ({}):", vote_events.events.len());
                            for event in &vote_events.events {
                                let tx_id = hex::encode(&event.tx_id);
                                println!("    tx_id: {}", tx_id);
                                println!("      index: {}", event.index);
                                println!("      status: {}", event.status);
                                if let Some(evt) = &event.event {
                                    println!("      event: {:?}", evt);
                                }
                                if let Some(p) = polls.get_mut(&tx_id) {
                                    let v = PollVote {
                                        sender_id: sender_id.clone(),
                                        payload_hash: event.event.clone().map(|e| match e {
                                            Event::ContractCallWithToken(c) => {
                                                hex::encode(&c.payload_hash)
                                            }
                                            _ => panic!("Unsupported event {e:?}"),
                                        }),
                                    };
                                    println!("found poll to store vote {v:?}");
                                    p.votes.push(v);
                                }
                            }
                        }
                    }
                    println!("done checking tx");
                }
                IoResult::ChainParams(chain) => {
                    println!("got chain params {:?}", chain);
                    chain_params_cache.insert(chain.chain.clone(), chain);
                }
            },
            ProcessingMessage::QueryMetrics(response_tx) => {
                let snapshot = MetricsSnapshot {
                    last_heartbeat: last_heartbeat.clone(),
                    last_visited_height,
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
        args[1].parse::<u32>().ok()
    } else {
        None
    };

    if let Some(height) = single_block {
        feeder_tx.send(IoCommand::FetchBlock(height)).unwrap();
        feeder_tx.send(IoCommand::FetchBlock(height + 1)).unwrap();
        thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(1));
            feeder_tx.send(IoCommand::Shutdown).unwrap();
        });
        //msg_tx.send(ProcessingMessage::Shutdown).unwrap();
    } else {
        thread::spawn(move || {
            loop {
                feeder_tx.send(IoCommand::FetchLatest).unwrap();
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
