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
    pub value: Option<String>,
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

#[derive(Deserialize, Debug)]
struct PollParticipantsJson {
    poll_id: String,
    // Ignore participants field - we don't need it
}

fn extract_poll_mappings_from_json(event: &TendermintEvent) -> Vec<PollMapping> {
    // TODO: we _could_ track participants (Vec<addr>)
    // to derive VP & poll completion
    // for now, we assume that after poll expiry, they all complete
    let mut poll_mappings = Vec::new();
    for attr in &event.attributes {
        let key_decoded = general_purpose::STANDARD.decode(&attr.key).ok();
        if let Some(key_bytes) = key_decoded
            && key_bytes == b"poll_mappings"
        {
            if let Some(value) = &attr.value {
                if let Ok(value_bytes) = general_purpose::STANDARD.decode(value) {
                    if let Ok(value_str) = String::from_utf8(value_bytes) {
                        if let Ok(mappings) =
                            serde_json::from_str::<Vec<PollMappingJson>>(&value_str)
                        {
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

#[derive(Debug)]
pub struct PollParticipants {
    pub tx_id: Vec<u8>,
    pub poll_id: u64,
    // TODO: extract asset from ConfirmDepositStarted event if needed
}

#[derive(Debug)]
pub enum PollEvent {
    GatewayTx { tx_id: Vec<u8>, poll_id: u64 },
    Deposit { tx_id: Vec<u8>, poll_id: u64 },
    TransferKey { tx_id: Vec<u8>, poll_id: u64 },
}
impl PollEvent {
    pub fn tx(&self) -> String {
        match self {
            PollEvent::Deposit { tx_id, .. } => hex::encode(tx_id),
            PollEvent::GatewayTx { tx_id, .. } => hex::encode(tx_id),
            PollEvent::TransferKey { tx_id, .. } => hex::encode(tx_id),
        }
    }
    pub fn poll_id(&self) -> u64 {
        match self {
            PollEvent::Deposit { poll_id, .. } => *poll_id,
            PollEvent::GatewayTx { poll_id, .. } => *poll_id,
            PollEvent::TransferKey { poll_id, .. } => *poll_id,
        }
    }
}

pub fn extract_poll_participants_from_events(
    block_results: &BlockResults,
    event_type: &str,
) -> Vec<PollParticipants> {
    let mut participants = Vec::new();
    if let Some(txs_results) = &block_results.txs_results {
        for tx_result in txs_results {
            if let Some(events) = &tx_result.events {
                for event in events {
                    if event.r#type == event_type {
                        if let Some(part) = extract_poll_participants_from_event(event) {
                            participants.push(part);
                        }
                    }
                }
            }
        }
    }
    participants
}

fn extract_poll_participants_from_event(event: &TendermintEvent) -> Option<PollParticipants> {
    let mut tx_id: Option<Vec<u8>> = None;
    let mut poll_id: Option<u64> = None;

    for attr in &event.attributes {
        if let Ok(key_bytes) = general_purpose::STANDARD.decode(&attr.key) {
            if let Ok(key_str) = String::from_utf8(key_bytes) {
                match key_str.as_str() {
                    "tx_id" => {
                        if let Some(value) = &attr.value {
                            if let Ok(value_bytes) = general_purpose::STANDARD.decode(value) {
                                if let Ok(value_str) = String::from_utf8(value_bytes) {
                                    if let Ok(json) = serde_json::from_str::<Vec<u8>>(&value_str) {
                                        tx_id = Some(json);
                                    }
                                }
                            }
                        }
                    }
                    "participants" => {
                        if let Some(value) = &attr.value {
                            if let Ok(value_bytes) = general_purpose::STANDARD.decode(value) {
                                if let Ok(value_str) = String::from_utf8(value_bytes) {
                                    if let Ok(json) =
                                        serde_json::from_str::<PollParticipantsJson>(&value_str)
                                    {
                                        poll_id = json.poll_id.parse::<u64>().ok();
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if let (Some(tx_id), Some(poll_id)) = (tx_id, poll_id) {
        Some(PollParticipants { tx_id, poll_id })
    } else {
        None
    }
}

pub fn extract_all_poll_events(block_results: &BlockResults) -> Vec<PollEvent> {
    let mut poll_events = Vec::new();

    if let Some(txs_results) = &block_results.txs_results {
        for tx_result in txs_results {
            if let Some(events) = &tx_result.events {
                for event in events {
                    let poll_event = match event.r#type.as_str() {
                        "axelar.evm.v1beta1.ConfirmGatewayTxStarted" => {
                            extract_poll_participants_from_event(event).map(|p| {
                                PollEvent::GatewayTx {
                                    tx_id: p.tx_id,
                                    poll_id: p.poll_id,
                                }
                            })
                        }
                        "axelar.evm.v1beta1.ConfirmDepositStarted" => {
                            extract_poll_participants_from_event(event).map(|p| {
                                PollEvent::Deposit {
                                    tx_id: p.tx_id,
                                    poll_id: p.poll_id,
                                }
                            })
                        }
                        "axelar.evm.v1beta1.ConfirmKeyTransferStarted" => {
                            extract_poll_participants_from_event(event).map(|p| {
                                PollEvent::TransferKey {
                                    tx_id: p.tx_id,
                                    poll_id: p.poll_id,
                                }
                            })
                        }
                        _ => None,
                    };

                    if let Some(pe) = poll_event {
                        poll_events.push(pe);
                    }
                }
            }
        }
    }

    poll_events
}
