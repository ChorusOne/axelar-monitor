use crate::blocks::parse_block;
use crate::config::Config;

mod blocks;
mod config;
mod generated;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load("config.toml")?;
    let url = format!("{}/cosmos/base/tendermint/v1beta1/blocks/latest", config.rpc_url);

    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let block = parse_block(&body)?;

    println!("Chain ID: {}", block.header.chain_id);
    println!("Height: {}", block.header.height);

    println!("\nTransactions:");
    let txs = blocks::get_txs(&block)?;
    for txbody in txs {
        let heartbeat_messages = blocks::extract_heartbeat_requests(&txbody);
        for hb in heartbeat_messages {
            let encoded_addr = hex::encode(&hb.sender);
            println!("HeartBeat sender: {}", encoded_addr);
            for bc in &config.broadcaster {
                if bc.address == encoded_addr {
                    println!("ours = {}", bc.name);
                }
            }
        }
    }

    Ok(())
}
