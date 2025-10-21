use crate::rpc::{BlockResults, TendermintEvent};
use base64::{Engine as _, engine::general_purpose};
use serde::Deserialize;

#[derive(Debug)]
pub struct PollVote {
    pub poll_id: u64,
    pub chain: String,
    pub tx_id: String,
    pub sender_id: String,
    pub payload_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PollKind {
    GatewayTx,
    Deposit { burner_address: String },
    TransferKey,
    Token,
}

#[derive(Debug, Clone)]
pub struct PollRequest {
    pub kind: PollKind,
    pub chain: String,
    pub tx: String,
}

impl PollRequest {
    pub fn with_expiry(self, expiry_height: u64) -> PollData {
        PollData {
            kind: self.kind,
            chain: self.chain,
            tx: self.tx,
            expiry_height,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollData {
    pub kind: PollKind,
    pub chain: String,
    pub tx: String,
    pub expiry_height: u64,
}

#[derive(Debug)]
pub struct Poll {
    pub poll_id: u64,
    pub data: PollData,
    pub votes: Vec<PollVote>,
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

fn decode_attribute_key(attr: &crate::rpc::EventAttribute) -> Option<String> {
    let key_bytes = general_purpose::STANDARD.decode(&attr.key).ok()?;
    String::from_utf8(key_bytes).ok()
}

fn decode_attribute_value<T: serde::de::DeserializeOwned>(
    attr: &crate::rpc::EventAttribute,
) -> Option<T> {
    let value = attr.value.as_ref()?;
    let value_bytes = general_purpose::STANDARD.decode(value).ok()?;
    let value_str = String::from_utf8(value_bytes).ok()?;
    serde_json::from_str(&value_str).ok()
}

fn extract_poll_mappings_from_json(event: &TendermintEvent) -> Vec<PollEvent> {
    let mut poll_mappings = Vec::new();
    for attr in &event.attributes {
        if let Some(key) = decode_attribute_key(attr)
            && key == "poll_mappings"
        {
            if let Some(mappings) = decode_attribute_value::<Vec<PollMappingJson>>(attr) {
                for mapping in mappings {
                    if let Ok(poll_id) = mapping.poll_id.parse::<u64>() {
                        poll_mappings.push(PollEvent {
                            kind: PollKind::GatewayTx,
                            tx_id: mapping.tx_id,
                            poll_id,
                        });
                    }
                }
            }
        }
    }
    poll_mappings
}

#[derive(Debug)]
pub struct PollEvent {
    pub kind: PollKind,
    pub tx_id: Vec<u8>,
    pub poll_id: u64,
}

fn extract_poll_event_from_attributes(
    event: &TendermintEvent,
    kind: PollKind,
) -> Option<PollEvent> {
    let mut tx_id: Option<Vec<u8>> = None;
    let mut poll_id: Option<u64> = None;

    for attr in &event.attributes {
        if let Some(key) = decode_attribute_key(attr) {
            match key.as_str() {
                "tx_id" => {
                    tx_id = decode_attribute_value::<Vec<u8>>(attr);
                }
                "participants" => {
                    if let Some(json) = decode_attribute_value::<PollParticipantsJson>(attr) {
                        poll_id = json.poll_id.parse::<u64>().ok();
                    }
                }
                _ => {}
            }
        }
    }

    if let (Some(tx_id), Some(poll_id)) = (tx_id, poll_id) {
        Some(PollEvent {
            kind,
            tx_id,
            poll_id,
        })
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
                            extract_poll_event_from_attributes(event, PollKind::GatewayTx)
                        }
                        "axelar.evm.v1beta1.ConfirmDepositStarted" => {
                            extract_poll_event_from_attributes(
                                event,
                                PollKind::Deposit {
                                    burner_address: String::new(),
                                },
                            )
                        }
                        "axelar.evm.v1beta1.ConfirmKeyTransferStarted" => {
                            extract_poll_event_from_attributes(event, PollKind::TransferKey)
                        }
                        "axelar.evm.v1beta1.ConfirmTokenStarted" => {
                            extract_poll_event_from_attributes(event, PollKind::Token)
                        }
                        _ => None,
                    };

                    if let Some(pe) = poll_event {
                        poll_events.push(pe);
                    }

                    if event.r#type == "axelar.evm.v1beta1.ConfirmGatewayTxsStarted" {
                        poll_events.append(&mut extract_poll_mappings_from_json(event));
                    }
                }
            }
        }
    }

    poll_events
}
