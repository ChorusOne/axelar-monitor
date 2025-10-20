use base64::{Engine as _, engine::general_purpose};
use cosmos_sdk_proto::cosmos::tx::v1beta1::{Tx, TxBody};
use prost::Message;
use serde::Deserialize;
use std::collections::HashMap;

use crate::config::ChainParams;
use crate::generated::axelar::evm::v1beta1::event::Event;
use crate::generated::axelar::evm::v1beta1::{
    ConfirmDepositRequest, ConfirmGatewayTxRequest, ConfirmGatewayTxsRequest,
    ConfirmTokenRequest, ConfirmTransferKeyRequest, VoteEvents,
};
use crate::generated::axelar::reward::v1beta1::RefundMsgRequest;
use crate::generated::axelar::tss::v1beta1::HeartBeatRequest;
use crate::generated::axelar::vote::v1beta1::VoteRequest;
use crate::polls::{PollCreation, PollRequest, PollVote};

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

pub enum RawPollRequests {
    GatewayTx(ConfirmGatewayTxRequest),
    GatewayTxs(ConfirmGatewayTxsRequest),
    Deposit(ConfirmDepositRequest),
    TransferKey(ConfirmTransferKeyRequest),
    Token(ConfirmTokenRequest),
}
pub fn extract_raw_poll_requests(txs: &[TxBody]) -> Vec<RawPollRequests> {
    txs.iter()
        .map(|tx| {
            tx.messages
                .iter()
                .filter_map(|msg| match msg.type_url.as_str() {
                    "/axelar.evm.v1beta1.ConfirmGatewayTxRequest" => {
                        Some(RawPollRequests::GatewayTx(
                            ConfirmGatewayTxRequest::decode(&msg.value[..]).unwrap(),
                        ))
                    }
                    // Tx<s> (plural)
                    "/axelar.evm.v1beta1.ConfirmGatewayTxsRequest" => {
                        Some(RawPollRequests::GatewayTxs(
                            ConfirmGatewayTxsRequest::decode(&msg.value[..]).unwrap(),
                        ))
                    }
                    "/axelar.evm.v1beta1.ConfirmDepositRequest" => Some(RawPollRequests::Deposit(
                        ConfirmDepositRequest::decode(&msg.value[..]).unwrap(),
                    )),
                    "/axelar.evm.v1beta1.ConfirmTransferKeyRequest" => {
                        Some(RawPollRequests::TransferKey(
                            ConfirmTransferKeyRequest::decode(&msg.value[..]).unwrap(),
                        ))
                    }
                    "/axelar.evm.v1beta1.ConfirmTokenRequest" => Some(RawPollRequests::Token(
                        ConfirmTokenRequest::decode(&msg.value[..]).unwrap(),
                    )),
                    _ => None,
                })
                .collect::<Vec<RawPollRequests>>()
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

pub struct RawBlockData {
    pub heartbeat_addrs: Vec<String>,
    pub poll_creations: Vec<PollCreation>,
    pub votes: Vec<PollVote>,
}

pub fn process_block(
    block: &Block,
    chain_params: &HashMap<String, ChainParams>,
    height: u64,
) -> Result<RawBlockData, Box<dyn std::error::Error>> {
    let txs = get_txs(block)?;

    let heartbeat_addrs: Vec<String> = txs
        .iter()
        .flat_map(|tx| extract_heartbeat_requests(tx))
        .map(|hb| hex::encode(&hb.sender))
        .collect();

    let raw_reqs = extract_raw_poll_requests(&txs);
    let poll_requests: Vec<PollRequest> = raw_reqs
        .iter()
        .map(|r| match r {
            RawPollRequests::GatewayTx(g) => vec![PollRequest::GatewayTx {
                tx: hex::encode(&g.tx_id),
                chain: g.chain.clone(),
            }],
            RawPollRequests::GatewayTxs(g) => g
                .tx_ids
                .iter()
                .map(|tx_id| PollRequest::GatewayTx {
                    tx: hex::encode(&tx_id),
                    chain: g.chain.clone(),
                })
                .collect(),
            RawPollRequests::Deposit(d) => vec![PollRequest::Deposit {
                tx: hex::encode(&d.tx_id),
                chain: d.chain.clone(),
                burner_address: hex::encode(&d.burner_address),
            }],
            RawPollRequests::TransferKey(t) => vec![PollRequest::TransferKey {
                tx: hex::encode(&t.tx_id),
                chain: t.chain.clone(),
            }],
            RawPollRequests::Token(t) => vec![PollRequest::Token {
                tx: hex::encode(&t.tx_id),
                chain: t.chain.clone(),
            }],
        })
        .flatten()
        .collect();

    let mut poll_creations = Vec::new();
    for request in poll_requests {
        let chain = request.chain();
        if let Some(params) = chain_params.get(&chain.to_lowercase()) {
            let revote_period = params.revote_locking_period as u64;
            let expiry_height = height + revote_period;

            let pc = match request {
                PollRequest::GatewayTx { chain, tx } => PollCreation::GatewayTx {
                    tx,
                    chain,
                    expiry_height,
                },
                PollRequest::Deposit {
                    chain,
                    tx,
                    burner_address,
                } => PollCreation::Deposit {
                    tx,
                    chain,
                    burner_address,
                    expiry_height,
                },
                PollRequest::TransferKey { chain, tx } => PollCreation::TransferKey {
                    tx,
                    chain,
                    expiry_height,
                },
                PollRequest::Token { chain, tx } => PollCreation::Token {
                    tx,
                    chain,
                    expiry_height,
                },
            };
            poll_creations.push(pc);
        }
    }

    let votes = get_votes_from_txs(&txs);

    Ok(RawBlockData {
        heartbeat_addrs,
        poll_creations,
        votes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChainParams;

    fn load_test_block(test_type: &str, height: u64) -> Block {
        let path = format!("test_data/{}/block_{}.json", test_type, height);
        let json = std::fs::read_to_string(&path).unwrap();
        parse_block(&json).unwrap()
    }

    fn create_test_chain_params() -> HashMap<String, ChainParams> {
        let mut params = HashMap::new();
        params.insert(
            "avalanche".to_string(),
            ChainParams {
                name: "Avalanche".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
        );
        params.insert(
            "scroll".to_string(),
            ChainParams {
                name: "scroll".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
        );
        params.insert(
            "binance".to_string(),
            ChainParams {
                name: "binance".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
        );
        params.insert(
            "ethereum".to_string(),
            ChainParams {
                name: "Ethereum".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
        );
        params
    }

    #[test]
    fn test_process_block_deposit() {
        let height = 20383480;
        let block = load_test_block("deposit", height);
        let chain_params = create_test_chain_params();

        let result = process_block(&block, &chain_params, height).unwrap();

        assert_eq!(result.poll_creations.len(), 1);

        match &result.poll_creations[0] {
            crate::polls::PollCreation::Deposit {
                tx,
                chain,
                burner_address,
                expiry_height,
            } => {
                assert_eq!(
                    tx,
                    "4af800f430dc829f3f08dd698dccdb3aac37438288653503bb2710f6cab386ec"
                );
                assert_eq!(chain, "Avalanche");
                assert_eq!(burner_address, "64db450dae5f15853b9119918cd7dd7944e67510");
                assert_eq!(*expiry_height, 20383480 + 15);
            }
            _ => panic!("Expected Deposit poll creation"),
        }
    }

    #[test]
    fn test_process_block_transfer_key() {
        let height = 20404088;
        let block = load_test_block("transfer_key", height);
        let chain_params = create_test_chain_params();

        let result = process_block(&block, &chain_params, height).unwrap();

        assert_eq!(result.poll_creations.len(), 2);

        let transfer_key = result
            .poll_creations
            .iter()
            .find(|pc| matches!(pc, crate::polls::PollCreation::TransferKey { .. }))
            .expect("Should have TransferKey poll creation");

        match transfer_key {
            crate::polls::PollCreation::TransferKey {
                tx,
                chain,
                expiry_height,
            } => {
                assert_eq!(
                    tx,
                    "78e2698855ffb323320c8d4ae1dc85eb3c8e10b3a75180a7aadb238f769f6e8d"
                );
                assert_eq!(chain, "scroll");
                assert_eq!(*expiry_height, 20404088 + 15);
            }
            _ => panic!("Expected TransferKey poll creation"),
        }

        let gateway_tx = result
            .poll_creations
            .iter()
            .find(|pc| matches!(pc, crate::polls::PollCreation::GatewayTx { .. }))
            .expect("Should have GatewayTx poll creation");

        match gateway_tx {
            crate::polls::PollCreation::GatewayTx {
                tx,
                chain,
                expiry_height,
            } => {
                assert_eq!(
                    tx,
                    "05a409afd25c53a59f98a48721a5274a450b4ac5a2007842b4e168a111af806e"
                );
                assert_eq!(chain, "scroll");
                assert_eq!(*expiry_height, 20404088 + 15);
            }
            _ => panic!("Expected GatewayTx poll creation"),
        }
    }

    #[test]
    fn test_process_block_gateway_txs_batch() {
        let height = 20413624;
        let block = load_test_block("gateway_txs", height);
        let chain_params = create_test_chain_params();

        let result = process_block(&block, &chain_params, height).unwrap();

        assert!(
            result.poll_creations.len() >= 1,
            "Should have at least one GatewayTx poll creation"
        );

        let has_binance_tx = result.poll_creations.iter().any(|pc| {
            if let crate::polls::PollCreation::GatewayTx { chain, .. } = pc {
                chain == "binance"
            } else {
                false
            }
        });

        assert!(
            has_binance_tx,
            "Should have a binance GatewayTx poll creation"
        );
    }

    #[test]
    fn test_process_block_confirm_token() {
        let height = 20133866;
        let block = load_test_block("confirm_token", height);
        let chain_params = create_test_chain_params();

        let result = process_block(&block, &chain_params, height).unwrap();

        assert_eq!(result.poll_creations.len(), 1);

        match &result.poll_creations[0] {
            crate::polls::PollCreation::Token {
                tx,
                chain,
                expiry_height,
            } => {
                assert_eq!(
                    tx,
                    "05f9266335faf6ff82f98687f7f19398426faf3b1cca5ef26b235b42baa593e5"
                );
                assert_eq!(chain, "Ethereum");
                assert_eq!(*expiry_height, 20133866 + 15);
            }
            _ => panic!("Expected Token poll creation"),
        }
    }
}
