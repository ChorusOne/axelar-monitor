use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Debug)]
pub struct ChainParams {
    pub name: String,
    pub revote_locking_period: u16,
    pub voting_grace_period: u16,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub rpc_url: String,
    pub lcd_url: String,
    pub poll_interval_seconds: u64,
    pub metrics_port: u16,
    pub broadcaster: Vec<Broadcaster>,
    pub chain_params: Vec<ChainParams>,
}

#[derive(Deserialize, Debug)]
pub struct Broadcaster {
    pub address: String,
    pub name: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}
