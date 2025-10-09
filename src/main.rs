use crate::blocks::{Block, parse_block};
use crate::config::{Broadcaster, Config};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod blocks;
mod config;
mod generated;

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
    result_tx: mpsc::Sender<IoResult>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(IoCommand::FetchLatest) => match get_block(&rpc_url, Height::Latest) {
                Ok(block) => {
                    if let Ok(height) = block.header.height.parse::<u32>() {
                        let _ = result_tx.send(IoResult::LatestFetched(height, block));
                    }
                }
                Err(e) => {
                    let _ = result_tx.send(IoResult::FetchError(Height::Latest, e.to_string()));
                }
            },
            Ok(IoCommand::FetchBlock(height)) => {
                match get_block(&rpc_url, Height::Specific(height)) {
                    Ok(block) => {
                        let _ = result_tx.send(IoResult::BlockFetched(height, block));
                    }
                    Err(e) => {
                        let _ = result_tx.send(IoResult::FetchError(
                            Height::Specific(height),
                            e.to_string(),
                        ));
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
    result_rx: mpsc::Receiver<IoResult>,
    cmd_tx: mpsc::Sender<IoCommand>,
    config: Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_visited_height = 0;
    let mut last_heartbeat: HashMap<String, u32> = config
        .broadcaster
        .iter()
        .map(|bc| (bc.name.clone(), 0))
        .collect();

    loop {
        match result_rx.recv()? {
            IoResult::LatestFetched(height, _block) => {
                let heights_to_check: Vec<u32> = heartbeat_blocks_for(height)
                    .iter()
                    .copied()
                    .filter(|b| *b > last_visited_height)
                    .collect();
                for height in &heights_to_check {
                    cmd_tx.send(IoCommand::FetchBlock(*height))?;
                }
                println!("Current height = {height}")
            }
            IoResult::FetchError(_, e) => {
                eprintln!("Error fetching latest: {}", e);
                continue;
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
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load("config.toml")?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<IoCommand>();
    let (result_tx, result_rx) = mpsc::channel::<IoResult>();

    let rpc_url = config.rpc_url.clone();
    let poll_interval = config.poll_interval_seconds;

    thread::spawn(move || {
        io_thread_loop(rpc_url, cmd_rx, result_tx);
    });

    let feeder_tx = cmd_tx.clone();
    thread::spawn(move || {
        loop {
            feeder_tx.send(IoCommand::FetchLatest).unwrap();
            thread::sleep(Duration::from_secs(poll_interval));
        }
    });

    processing_loop(result_rx, cmd_tx, config)
}
