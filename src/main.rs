use crate::blocks::{Block, parse_block};
use crate::config::Config;
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

fn process_block(block: &Block, config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let txs = blocks::get_txs(block)?;
    for txbody in txs {
        let heartbeat_messages = blocks::extract_heartbeat_requests(&txbody);
        for hb in heartbeat_messages {
            let encoded_addr = hex::encode(&hb.sender);
            for bc in &config.broadcaster {
                if bc.address == encoded_addr {
                    println!(
                        "Height {}: HeartBeat sender: {}",
                        block.header.height, encoded_addr
                    );
                    println!("  -> {} heartbeat detected", bc.name);
                    break;
                }
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load("config.toml")?;

    let initial_block = get_block(&config.rpc_url, Height::Latest)?;
    let initial_height: u32 = initial_block.header.height.parse()?;

    println!("Starting from height: {}", initial_height);
    println!("Chain ID: {}", initial_block.header.chain_id);

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
                process_block(&block, &config)?;
            }
            Err(e) => {
                eprintln!("Error fetching height {}: {}", height, e);
            }
        }
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
                        process_block(&block, &config)?;
                    }
                    Err(e) => {
                        eprintln!("Error fetching height {}: {}", height, e);
                    }
                }
            }

            last_visited_height = latest_height;
        }
        println!("Processed block {}", latest_height);
    }
}
