use base64::{Engine as _, engine::general_purpose};
use serde::Deserialize;

use crate::generated::axelar::evm::v1beta1::PollMapping;

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
    pub value: String,
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

#[derive(Deserialize, Debug)]
struct PollMappingJson {
    tx_id: Vec<u8>,
    poll_id: String,
}

fn extract_poll_mappings_from_json(event: &TendermintEvent) -> Vec<PollMapping> {
    let mut poll_mappings = Vec::new();
    for attr in &event.attributes {
        let key_decoded = general_purpose::STANDARD.decode(&attr.key).ok();
        if let Some(key_bytes) = key_decoded
            && key_bytes == b"poll_mappings"
        {
            if let Ok(value_bytes) = general_purpose::STANDARD.decode(&attr.value) {
                if let Ok(value_str) = String::from_utf8(value_bytes) {
                    if let Ok(mappings) = serde_json::from_str::<Vec<PollMappingJson>>(&value_str) {
                        for mapping in mappings {
                            if let Ok(poll_id) = mapping.poll_id.parse::<u64>() {
                                poll_mappings.push(PollMapping {
                                    tx_id: mapping.tx_id,
                                    poll_id,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    poll_mappings
}
pub fn extract_poll_mappings_from_events(block_results: &BlockResults) -> Vec<PollMapping> {
    let mut poll_mappings = Vec::new();
    if let Some(txs_results) = &block_results.txs_results {
        for tx_result in txs_results {
            if let Some(events) = &tx_result.events {
                for event in events {
                    if event.r#type == "axelar.evm.v1beta1.ConfirmGatewayTxsStarted" {
                        poll_mappings.append(&mut extract_poll_mappings_from_json(event));
                    }
                }
            }
        }
    }

    poll_mappings
}
