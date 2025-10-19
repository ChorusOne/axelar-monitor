use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub rpc_url: String,
    pub lcd_url: String,
    pub poll_interval_seconds: u64,
    pub metrics_port: u16,
    pub broadcaster: Vec<Broadcaster>,
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
