use crate::blocks::{Block, parse_block};
use crate::config::{Broadcaster, Config};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

mod blocks;
mod config;
mod generated;

enum Height {
    Latest,
    Specific(u32),
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load("config.toml")?;

    let initial_block = get_block(&config.rpc_url, Height::Latest)?;
    let initial_height: u32 = initial_block.header.height.parse()?;

    println!("Starting from height: {}", initial_height);
    println!("Chain ID: {}", initial_block.header.chain_id);

    let mut last_heartbeat: HashMap<String, u32> = config
        .broadcaster
        .iter()
        .map(|bc| (bc.name.clone(), initial_height.saturating_sub(100)))
        .collect();

    let mut last_visited_height = 0;

    loop {
        let latest_block = get_block(&config.rpc_url, Height::Latest)?;
        let latest_height: u32 = latest_block.header.height.parse()?;

        let heights_to_check: Vec<u32> = heartbeat_blocks_for(latest_height)
            .iter()
            .copied()
            .filter(|b| *b > last_visited_height)
            .collect();

        println!("highest was {}", latest_height);
        for height in heights_to_check {
            println!("\nChecking {:?}", height);
            match get_block(&config.rpc_url, Height::Specific(height)) {
                Ok(block) => {
                    let matched = process_block(&block, &config)?;
                    let block_height: u32 = block.header.height.parse()?;
                    for broadcaster in matched {
                        println!(
                            "Height {}: {} heartbeat detected",
                            block_height, broadcaster.name
                        );
                        last_heartbeat.insert(broadcaster.name.clone(), block_height);
                    }
                }
                Err(e) => {
                    eprintln!("Error fetching height {}: {}", height, e);
                }
            }

            last_visited_height = height;
        }

        thread::sleep(Duration::from_secs(6));
    }
}
