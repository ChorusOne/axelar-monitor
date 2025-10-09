use crate::blocks::{Block, parse_block};
use crate::config::{Broadcaster, Config};
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
    #[allow(dead_code)]
    Shutdown,
}

enum IoResult {
    LatestFetched(u32, Block),
    BlockFetched(u32, Block),
    FetchError(Height, String),
}

#[derive(Debug)]
struct MetricsSnapshot {
    last_heartbeat: HashMap<String, u32>,
    last_visited_height: u32,
    chain_height: u32,
    fetch_error_count: u64,
}

enum ProcessingMessage {
    BlockResult(IoResult),
    QueryMetrics(mpsc::Sender<MetricsSnapshot>),
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
                        .send(ProcessingMessage::BlockResult(IoResult::LatestFetched(
                            height, block,
                        )))
                        .unwrap();
                }
                Err(e) => {
                    msg_tx
                        .send(ProcessingMessage::BlockResult(IoResult::FetchError(
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
                            .send(ProcessingMessage::BlockResult(IoResult::BlockFetched(
                                height, block,
                            )))
                            .unwrap();
                    }
                    Err(e) => {
                        msg_tx
                            .send(ProcessingMessage::BlockResult(IoResult::FetchError(
                                Height::Specific(height),
                                e.to_string(),
                            )))
                            .unwrap();
                    }
                }
            }
            Ok(IoCommand::Shutdown) => break,
            Err(_) => break,
        }
    }
}

fn process_block<'a>(
    block: &Block,
    config: &'a Config,
) -> Result<Vec<&'a Broadcaster>, Box<dyn std::error::Error>> {
    let txs = blocks::get_txs(block)?;
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
    Ok(ret)
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

    loop {
        match msg_rx.recv()? {
            ProcessingMessage::BlockResult(result) => match result {
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
                IoResult::BlockFetched(height, block) => {
                    println!("\nChecking {:?}", height);
                    let matched = process_block(&block, &config)?;
                    for broadcaster in matched {
                        println!("Height {}: {} heartbeat detected", height, broadcaster.name);
                        last_heartbeat.insert(broadcaster.name.clone(), height);
                    }
                    last_visited_height = std::cmp::max(last_visited_height, height);
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
        }
    }
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

    processing_loop(msg_rx, cmd_tx, config)
}
