use crate::Height;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct BlockResultsResponse {
    pub result: BlockResults,
}

#[derive(Deserialize, Debug)]
pub struct BlockResults {
    pub txs_results: Option<Vec<TxResult>>,
}

#[derive(Deserialize, Debug)]
pub struct TxResult {
    pub events: Option<Vec<TendermintEvent>>,
}

#[derive(Deserialize, Debug)]
pub struct TendermintEvent {
    pub r#type: String,
    pub attributes: Vec<EventAttribute>,
}

#[derive(Deserialize, Debug)]
pub struct EventAttribute {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ChainListResponse {
    pub chains: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct ChainParamsResponse {
    pub params: ChainParamsJson,
}

#[derive(Deserialize, Debug)]
pub struct ChainParamsJson {
    pub chain: String,
    pub revote_locking_period: String,
    pub voting_grace_period: String,
}

impl Into<crate::config::ChainParams> for ChainParamsJson {
    fn into(self) -> crate::config::ChainParams {
        crate::config::ChainParams {
            name: self.chain,
            revote_locking_period: self.revote_locking_period.parse().unwrap(),
            voting_grace_period: self.voting_grace_period.parse().unwrap(),
        }
    }
}

pub fn get_block(
    base_url: &str,
    height: Height,
) -> Result<(crate::blocks::Block, u64), Box<dyn std::error::Error>> {
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
    let b = crate::blocks::parse_block(&body)?;
    let h = b.header.height.parse()?;
    Ok((b, h))
}

pub fn get_head(base_url: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let url = format!("{}/cosmos/base/tendermint/v1beta1/blocks/latest", base_url);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let b = crate::blocks::parse_block(&body)?;
    let h = b.header.height.parse()?;
    Ok(h)
}

pub fn get_chain_list(base_url: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let url = format!("{}/axelar/evm/v1beta1/chains", base_url);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: ChainListResponse = serde_json::from_str(&body)?;
    Ok(parsed.chains)
}

pub fn get_chain_params(
    base_url: &str,
    chain: &str,
) -> Result<ChainParamsJson, Box<dyn std::error::Error>> {
    let url = format!("{}/axelar/evm/v1beta1/params/{}", base_url, chain);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: ChainParamsResponse = serde_json::from_str(&body)?;
    Ok(parsed.params)
}

pub fn get_block_results(
    lcd_url: &str,
    height: u64,
) -> Result<BlockResults, Box<dyn std::error::Error>> {
    let url = format!("{}/block_results?height={}", lcd_url, height);
    let mut response = ureq::get(&url).call()?;
    let body = response.body_mut().read_to_string()?;
    let parsed: BlockResultsResponse = serde_json::from_str(&body)?;
    Ok(parsed.result)
}
