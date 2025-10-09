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

fn should_check_height(height: u32) -> bool {
    let remainder = height % 50;
    remainder == 0 || remainder == 1 || remainder == 2
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

    let previous_cycle_base = (initial_height / 50) * 50;
    let heights_to_backcheck: Vec<u32> = (previous_cycle_base..=previous_cycle_base + 2)
        .filter(|h| *h <= initial_height)
        .collect();

    println!(
        "\nBackchecking previous heartbeat heights: {:?}",
        heights_to_backcheck
    );
    for height in heights_to_backcheck {
        match get_block(&config.rpc_url, Height::Specific(height)) {
            Ok(block) => {
                let matched = process_block(&block, &config)?;
                let block_height: u32 = block.header.height.parse()?;
                for broadcaster in matched {
                    println!("Height {}: {} heartbeat detected", block_height, broadcaster.name);
                    last_heartbeat.insert(broadcaster.name.clone(), block_height);
                }
            }
            Err(e) => {
                eprintln!("Error fetching height {}: {}", height, e);
            }
        }
    }

    println!("\nLast heartbeat status:");
    for (name, height) in &last_heartbeat {
        println!("  {}: {}", name, height);
    }

    let mut last_visited_height = initial_height;

    loop {
        thread::sleep(Duration::from_secs(6));

        let latest_block = get_block(&config.rpc_url, Height::Latest)?;
        let latest_height: u32 = latest_block.header.height.parse()?;

        if latest_height > last_visited_height {
            let heights_to_check: Vec<u32> = ((last_visited_height + 1)..=latest_height)
                .filter(|h| should_check_height(*h))
                .collect();

            if !heights_to_check.is_empty() {
                println!("\nChecking heights: {:?}", heights_to_check);
            }

            for height in heights_to_check {
                match get_block(&config.rpc_url, Height::Specific(height)) {
                    Ok(block) => {
                        let matched = process_block(&block, &config)?;
                        let block_height: u32 = block.header.height.parse()?;
                        for broadcaster in matched {
                            println!("Height {}: {} heartbeat detected", block_height, broadcaster.name);
                            last_heartbeat.insert(broadcaster.name.clone(), block_height);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error fetching height {}: {}", height, e);
                    }
                }
            }

            last_visited_height = latest_height;
        }
    }
}
