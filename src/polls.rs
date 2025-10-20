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

#[derive(Debug)]
pub enum PollRequest {
    GatewayTx {
        tx: String,
        chain: String,
    },
    Deposit {
        tx: String,
        chain: String,
        burner_address: String,
    },
    TransferKey {
        tx: String,
        chain: String,
    },
    Token {
        tx: String,
        chain: String,
    },
}

impl PollRequest {
    fn tx(&self) -> &str {
        match self {
            PollRequest::GatewayTx { tx, .. } => tx,
            PollRequest::Deposit { tx, .. } => tx,
            PollRequest::TransferKey { tx, .. } => tx,
            PollRequest::Token { tx, .. } => tx,
        }
    }

    pub fn chain(&self) -> &str {
        match self {
            PollRequest::GatewayTx { chain, .. } => chain,
            PollRequest::Deposit { chain, .. } => chain,
            PollRequest::TransferKey { chain, .. } => chain,
            PollRequest::Token { chain, .. } => chain,
        }
    }
}

#[derive(Debug)]
pub enum PollType {
    GatewayTx {
        chain: String,
        tx: String,
    },
    Deposit {
        chain: String,
        tx: String,
        burner_address: String,
        // TODO: add asset field if needed (extract from ConfirmDepositStarted event)
    },
    TransferKey {
        chain: String,
        tx: String,
    },
}

#[derive(Debug)]
pub enum PollCreation {
    GatewayTx {
        tx: String,
        expiry_height: u64,
        chain: String,
    },
    Deposit {
        tx: String,
        expiry_height: u64,
        chain: String,
        burner_address: String,
    },
    TransferKey {
        tx: String,
        expiry_height: u64,
        chain: String,
    },
    Token {
        tx: String,
        expiry_height: u64,
        chain: String,
    },
}

impl PollCreation {
    pub fn into_poll(&self, poll_id: u64, tx: String) -> Poll {
        Poll {
            poll_id,
            poll_type: self.into_polltype(tx),
            votes: vec![],
            expiry_height: self.expiry_height(),
        }
    }

    fn into_polltype(&self, tx: String) -> PollType {
        match &self {
            PollCreation::GatewayTx { chain, .. } => PollType::GatewayTx {
                chain: chain.clone(),
                tx,
            },
            PollCreation::Deposit {
                chain,
                burner_address,
                ..
            } => PollType::Deposit {
                chain: chain.clone(),
                tx,
                burner_address: burner_address.clone(),
            },
            PollCreation::TransferKey { chain, .. } => PollType::TransferKey {
                chain: chain.clone(),
                tx,
            },
            PollCreation::Token { chain, .. } => PollType::GatewayTx {
                chain: chain.clone(),
                tx,
            },
        }
    }
    pub fn tx(&self) -> &str {
        match self {
            PollCreation::GatewayTx { tx, .. } => tx,
            PollCreation::Deposit { tx, .. } => tx,
            PollCreation::TransferKey { tx, .. } => tx,
            PollCreation::Token { tx, .. } => tx,
        }
    }

    fn expiry_height(&self) -> u64 {
        match self {
            PollCreation::GatewayTx { expiry_height, .. } => *expiry_height,
            PollCreation::Deposit { expiry_height, .. } => *expiry_height,
            PollCreation::TransferKey { expiry_height, .. } => *expiry_height,
            PollCreation::Token { expiry_height, .. } => *expiry_height,
        }
    }
}

#[derive(Debug)]
pub struct Poll {
    pub poll_id: u64,
    pub poll_type: PollType,
    pub votes: Vec<PollVote>,
    pub expiry_height: u64,
}

impl Poll {
    fn tx(&self) -> &str {
        match &self.poll_type {
            PollType::GatewayTx { tx, .. } => tx,
            PollType::Deposit { tx, .. } => tx,
            PollType::TransferKey { tx, .. } => tx,
        }
    }

    fn chain(&self) -> &str {
        match &self.poll_type {
            PollType::GatewayTx { chain, .. } => chain,
            PollType::Deposit { chain, .. } => chain,
            PollType::TransferKey { chain, .. } => chain,
        }
    }
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

fn extract_poll_mappings_from_json(event: &TendermintEvent) -> Vec<PollEvent> {
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
                                    poll_mappings.push(PollEvent::GatewayTx {
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
    Token { tx_id: Vec<u8>, poll_id: u64 },
}
impl PollEvent {
    pub fn tx(&self) -> String {
        match self {
            PollEvent::Deposit { tx_id, .. } => hex::encode(tx_id),
            PollEvent::GatewayTx { tx_id, .. } => hex::encode(tx_id),
            PollEvent::TransferKey { tx_id, .. } => hex::encode(tx_id),
            PollEvent::Token { tx_id, .. } => hex::encode(tx_id),
        }
    }
    pub fn poll_id(&self) -> u64 {
        match self {
            PollEvent::Deposit { poll_id, .. } => *poll_id,
            PollEvent::GatewayTx { poll_id, .. } => *poll_id,
            PollEvent::TransferKey { poll_id, .. } => *poll_id,
            PollEvent::Token { poll_id, .. } => *poll_id,
        }
    }
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
                        "axelar.evm.v1beta1.ConfirmTokenStarted" => {
                            extract_poll_participants_from_event(event).map(|p| PollEvent::Token {
                                tx_id: p.tx_id,
                                poll_id: p.poll_id,
                            })
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
