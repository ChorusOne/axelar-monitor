use base64::{Engine as _, engine::general_purpose};
use cosmos_sdk_proto::cosmos::tx::v1beta1::{Tx, TxBody};
use prost::Message;
use serde::Deserialize;

use crate::PollVote;
use crate::generated::axelar::evm::v1beta1::event::Event;
use crate::generated::axelar::evm::v1beta1::{ConfirmDepositRequest, ConfirmGatewayTxRequest, ConfirmGatewayTxsRequest, VoteEvents};
use crate::generated::axelar::reward::v1beta1::RefundMsgRequest;
use crate::generated::axelar::tss::v1beta1::HeartBeatRequest;
use crate::generated::axelar::vote::v1beta1::VoteRequest;

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

pub struct DecodedVote {
    pub sender: Vec<u8>,
    pub poll_id: u64,
    pub vote_events: Option<VoteEvents>,
}

pub fn extract_decoded_votes(txs: &[TxBody]) -> Vec<DecodedVote> {
    txs.iter()
        .map(|tx| extract_decoded_votes_from_tx(tx))
        .flatten()
        .collect()
}

fn extract_decoded_votes_from_tx(tx: &TxBody) -> Vec<DecodedVote> {
    let vote_requests = extract_vote_requests(tx);
    vote_requests
        .into_iter()
        .map(|vote| {
            let vote_events = vote.vote.as_ref().and_then(|vote_data| {
                if vote_data.type_url == "/axelar.evm.v1beta1.VoteEvents" {
                    VoteEvents::decode(&vote_data.value[..]).ok()
                } else {
                    None
                }
            });

            DecodedVote {
                sender: vote.sender,
                poll_id: vote.poll_id,
                vote_events,
            }
        })
        .collect()
}

pub fn extract_vote_requests(tx: &TxBody) -> Vec<VoteRequest> {
    let refund_messages = extract_refund_messages(tx);
    refund_messages
        .iter()
        .filter_map(|rm| extract_vote_request(rm))
        .collect()
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

pub fn print_all_refund_inner_message_types(tx: &TxBody) {
    let refund_messages = extract_refund_messages(tx);
    for refund_msg in refund_messages {
        if let Ok(refund) = RefundMsgRequest::decode(&refund_msg.value[..]) {
            if let Some(inner) = refund.inner_message {
                println!(
                    "  RefundMsgRequest.inner_message.type_url: {}",
                    inner.type_url
                );
            }
        }
    }
}

pub fn extract_confirm_gateway_tx_requests(txs: &[TxBody]) -> Vec<ConfirmGatewayTxRequest> {
    txs.iter()
        .map(|tx| {
            tx.messages
                .iter()
                .filter(|msg| msg.type_url == "/axelar.evm.v1beta1.ConfirmGatewayTxRequest")
                .filter_map(|msg| ConfirmGatewayTxRequest::decode(&msg.value[..]).ok())
                .collect::<Vec<ConfirmGatewayTxRequest>>()
        })
        .flatten()
        .collect()
}

pub fn extract_confirm_gateway_txs_requests(txs: &[TxBody]) -> Vec<ConfirmGatewayTxsRequest> {
    txs.iter()
        .map(|tx| {
            tx.messages
                .iter()
                .filter(|msg| msg.type_url == "/axelar.evm.v1beta1.ConfirmGatewayTxsRequest")
                .filter_map(|msg| ConfirmGatewayTxsRequest::decode(&msg.value[..]).ok())
                .collect::<Vec<ConfirmGatewayTxsRequest>>()
        })
        .flatten()
        .collect()
}

pub fn extract_confirm_deposit_requests(txs: &[TxBody]) -> Vec<ConfirmDepositRequest> {
    txs.iter()
        .map(|tx| {
            tx.messages
                .iter()
                .filter(|msg| msg.type_url == "/axelar.evm.v1beta1.ConfirmDepositRequest")
                .filter_map(|msg| ConfirmDepositRequest::decode(&msg.value[..]).ok())
                .collect::<Vec<ConfirmDepositRequest>>()
        })
        .flatten()
        .collect()
}

fn extract_vote_request(refund_msg: &cosmos_sdk_proto::Any) -> Option<VoteRequest> {
    let refund = RefundMsgRequest::decode(&refund_msg.value[..]).ok()?;
    let inner = refund.inner_message?;

    if inner.type_url != "/axelar.vote.v1beta1.VoteRequest" {
        return None;
    }

    VoteRequest::decode(&inner.value[..]).ok()
}

fn extract_heartbeat_request(refund_msg: &cosmos_sdk_proto::Any) -> Option<HeartBeatRequest> {
    let refund = RefundMsgRequest::decode(&refund_msg.value[..]).ok()?;
    let inner = refund.inner_message?;

    if inner.type_url != "/axelar.tss.v1beta1.HeartBeatRequest" {
        return None;
    }

    HeartBeatRequest::decode(&inner.value[..]).ok()
}

pub fn get_votes_from_txs(txs: &[TxBody]) -> Vec<PollVote> {
    let mut ret: Vec<_> = vec![];
    for vote in extract_decoded_votes(&txs) {
        let sender_id = hex::encode(&vote.sender);
        if let Some(vote_events) = &vote.vote_events {
            if vote_events.events.is_empty() {
                let v = PollVote {
                    poll_id: vote.poll_id,
                    chain: vote_events.chain.clone(),
                    tx_id: String::new(),
                    sender_id: sender_id.clone(),
                    payload_hash: None,
                };
                ret.push(v);
            } else {
                for event in &vote_events.events {
                    let tx_id = hex::encode(&event.tx_id);
                    let v = PollVote {
                        poll_id: vote.poll_id,
                        chain: vote_events.chain.clone(),
                        tx_id: tx_id,
                        sender_id: sender_id.clone(),
                        payload_hash: match &event.event {
                            Some(Event::ContractCall(c)) => Some(hex::encode(&c.payload_hash)),
                            Some(Event::ContractCallWithToken(c)) => {
                                Some(hex::encode(&c.payload_hash))
                            }
                            Some(Event::MultisigOperatorshipTransferred(_)) => None,
                            Some(Event::Transfer(_)) => None,
                            Some(u) => panic!("Unsupported event {u:?}"),
                            None => None,
                        },
                    };
                    ret.push(v);
                }
            }
        } else {
            println!("no events??");
            let v = PollVote {
                poll_id: vote.poll_id,
                chain: String::new(),
                tx_id: String::new(),
                sender_id: sender_id.clone(),
                payload_hash: None,
            };
            ret.push(v);
        }
    }
    ret
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
