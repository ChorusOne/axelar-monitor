use base64::{Engine as _, engine::general_purpose};
use cosmos_sdk_proto::cosmos::tx::v1beta1::{Tx, TxBody};
use prost::Message;
use serde::Deserialize;

use crate::generated::axelar::reward::v1beta1::RefundMsgRequest;
use crate::generated::axelar::tss::v1beta1::HeartBeatRequest;

#[derive(Deserialize, Debug)]
struct Response {
    block: Block,
}

#[derive(Deserialize, Debug)]
pub struct Block {
    pub header: Header,
    pub data: Data,
}

#[derive(Deserialize, Debug)]
pub struct Data {
    pub txs: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct Header {
    pub height: String,
}

pub fn extract_heartbeat_requests(tx: &TxBody) -> Vec<HeartBeatRequest> {
    let refund_messages = extract_refund_messages(tx);
    let heartbeat_messages: Vec<HeartBeatRequest> = refund_messages
        .iter()
        .filter_map(|rm| extract_heartbeat_request(rm))
        .collect();
    heartbeat_messages
}
fn extract_refund_messages(tx_body: &TxBody) -> Vec<&cosmos_sdk_proto::Any> {
    tx_body
        .messages
        .iter()
        .filter(|msg| msg.type_url == "/axelar.reward.v1beta1.RefundMsgRequest")
        .collect()
}

fn extract_heartbeat_request(refund_msg: &cosmos_sdk_proto::Any) -> Option<HeartBeatRequest> {
    let refund = RefundMsgRequest::decode(&refund_msg.value[..]).ok()?;
    let inner = refund.inner_message?;

    if inner.type_url != "/axelar.tss.v1beta1.HeartBeatRequest" {
        return None;
    }

    HeartBeatRequest::decode(&inner.value[..]).ok()
}

pub fn parse_block(json_data: &str) -> Result<Block, Box<dyn std::error::Error>> {
    let data: Response = serde_json::from_str(json_data)?;
    Ok(data.block)
}

pub fn get_txs(block: &Block) -> Result<Vec<TxBody>, Box<dyn std::error::Error>> {
    let mut ret = Vec::new();

    for (idx, tx_base64) in block.data.txs.iter().enumerate() {
        match general_purpose::STANDARD.decode(tx_base64) {
            Ok(tx_bytes) => match Tx::decode(&tx_bytes[..]) {
                Ok(tx) => {
                    if let Some(body) = tx.body {
                        ret.push(body);
                    }
                }
                Err(e) => {
                    println!("Tx {}: Failed to parse protobuf: {}", idx, e);
                }
            },
            Err(e) => {
                println!("Tx {}: Failed to decode base64: {}", idx, e);
            }
        }
    }

    Ok(ret)
}
