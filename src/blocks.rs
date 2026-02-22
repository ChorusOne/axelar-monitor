use base64::{Engine as _, engine::general_purpose};
use cosmos_sdk_proto::cosmos::tx::v1beta1::{Tx, TxBody};
use log::info;
use prost::Message;
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::config::ChainParams;
use crate::generated::axelar::evm::v1beta1::event::{self, Event};
use crate::generated::axelar::evm::v1beta1::{
    ConfirmDepositRequest, ConfirmGatewayTxRequest, ConfirmGatewayTxsRequest, ConfirmTokenRequest,
    ConfirmTransferKeyRequest, VoteEvents,
};
use crate::generated::axelar::reward::v1beta1::RefundMsgRequest;
use crate::generated::axelar::vote::v1beta1::VoteRequest;
use crate::polls::{PollData, PollKind, PollRequest, PollVote};

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

fn decode_bech32_to_hex(bech32_addr: &str) -> Option<String> {
    match bech32::decode(bech32_addr) {
        Ok((_hrp, data)) => Some(hex::encode(&data)),
        Err(e) => {
            log::error!("Failed to decode bech32 address '{}': {}", bech32_addr, e);
            None
        }
    }
}

pub struct DecodedVote {
    pub sender: String,
    pub poll_id: u64,
    pub vote_events: Option<VoteEvents>,
}

pub fn extract_decoded_votes(txs: &[TxBody]) -> Vec<DecodedVote> {
    txs.iter().flat_map(extract_decoded_votes_from_tx).collect()
}

fn extract_decoded_votes_from_tx(tx: &TxBody) -> Vec<DecodedVote> {
    let vote_requests = extract_vote_requests(tx);
    vote_requests
        .into_iter()
        .filter_map(|vote| {
            let vote_events = vote.vote.as_ref().and_then(|vote_data| {
                if vote_data.type_url == "/axelar.evm.v1beta1.VoteEvents" {
                    VoteEvents::decode(&vote_data.value[..]).ok()
                } else {
                    None
                }
            });

            let sender = decode_bech32_to_hex(&vote.sender)?;

            Some(DecodedVote {
                sender,
                poll_id: vote.poll_id,
                vote_events,
            })
        })
        .collect()
}

pub fn extract_vote_requests(tx: &TxBody) -> Vec<VoteRequest> {
    let refund_messages = extract_refund_messages(tx);
    refund_messages
        .iter()
        .filter_map(extract_vote_request)
        .collect()
}

fn extract_refund_messages(tx_body: &TxBody) -> Vec<cosmos_sdk_proto::Any> {
    let mut refund_messages = Vec::new();

    for msg in &tx_body.messages {
        if msg.type_url == "/axelar.reward.v1beta1.RefundMsgRequest" {
            refund_messages.push(msg.clone());
        } else if msg.type_url == "/axelar.auxiliary.v1beta1.BatchRequest" {
            if let Ok(batch) = decode_batch_request(&msg.value) {
                for inner_msg in batch {
                    if inner_msg.type_url == "/axelar.reward.v1beta1.RefundMsgRequest" {
                        refund_messages.push(inner_msg);
                    }
                }
            }
        }
    }

    refund_messages
}

fn decode_batch_request(data: &[u8]) -> Result<Vec<cosmos_sdk_proto::Any>, prost::DecodeError> {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    struct BatchRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub sender: Vec<u8>,
        #[prost(message, repeated, tag = "2")]
        pub messages: Vec<cosmos_sdk_proto::Any>,
    }

    let batch = BatchRequest::decode(data)?;
    Ok(batch.messages)
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
        .flat_map(|tx| {
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

fn event_name(evt: &Option<event::Event>) -> &str {
    match evt {
        Some(Event::ContractCall(_)) => "ContractCall",
        Some(Event::ContractCallWithToken(_)) => "ContractCallWithToken",
        Some(Event::MultisigOperatorshipTransferred(_)) => "MultisigOperatorshipTransferred",
        Some(Event::MultisigOwnershipTransferred(_)) => "MultisigOwnershipTransferred",
        Some(Event::Transfer(_)) => "Transfer",
        Some(Event::TokenSent(_)) => "TokenSent",
        Some(Event::TokenDeployed(_)) => "TokenDeployed",
        None => "None",
    }
}

pub fn get_votes_from_txs(txs: &[TxBody]) -> Vec<PollVote> {
    let mut ret: Vec<_> = vec![];
    for vote in extract_decoded_votes(txs) {
        let sender_id = vote.sender.clone();
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
                        tx_id,
                        sender_id: sender_id.clone(),
                        payload_hash: match &event.event {
                            Some(Event::ContractCall(c)) => Some(hex::encode(&c.payload_hash)),
                            Some(Event::ContractCallWithToken(c)) => {
                                Some(hex::encode(&c.payload_hash))
                            }
                            Some(Event::MultisigOperatorshipTransferred(_)) => None,
                            Some(Event::MultisigOwnershipTransferred(_)) => None,
                            Some(Event::Transfer(_)) => None,
                            Some(Event::TokenSent(_)) => None,
                            Some(Event::TokenDeployed(_)) => None,
                            None => None,
                        },
                    };

                    if v.payload_hash.is_none() {
                        info!(
                            "On poll {}, sender {}, hash for {} is None",
                            vote.poll_id,
                            &sender_id,
                            event_name(&event.event),
                        );
                    }

                    ret.push(v);
                }
            }
        } else {
            log::warn!(
                "Skipping vote on poll {} from {}: not a VoteEvents message",
                vote.poll_id,
                sender_id
            );
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
                    log::warn!("Tx {}: Failed to parse protobuf: {}", idx, e);
                }
            },
            Err(e) => {
                log::warn!("Tx {}: Failed to decode base64: {}", idx, e);
            }
        }
    }

    Ok(ret)
}

pub struct RawBlockData {
    pub poll_creations: Vec<PollData>,
    pub votes: Vec<PollVote>,
}

pub fn process_block(
    block: &Block,
    chain_params: &BTreeMap<String, ChainParams>,
    height: u64,
) -> Result<RawBlockData, Box<dyn std::error::Error>> {
    let txs = get_txs(block)?;

    let raw_reqs = extract_raw_poll_requests(&txs);
    let poll_requests: Vec<PollRequest> = raw_reqs
        .iter()
        .flat_map(|r| match r {
            RawPollRequests::GatewayTx(g) => vec![PollRequest {
                kind: PollKind::GatewayTx,
                tx: hex::encode(&g.tx_id),
                chain: g.chain.clone(),
            }],
            RawPollRequests::GatewayTxs(g) => g
                .tx_ids
                .iter()
                .map(|tx_id| PollRequest {
                    kind: PollKind::GatewayTx,
                    tx: hex::encode(tx_id),
                    chain: g.chain.clone(),
                })
                .collect(),
            RawPollRequests::Deposit(d) => vec![PollRequest {
                kind: PollKind::Deposit {
                    burner_address: hex::encode(&d.burner_address),
                },
                tx: hex::encode(&d.tx_id),
                chain: d.chain.clone(),
            }],
            RawPollRequests::TransferKey(t) => vec![PollRequest {
                kind: PollKind::TransferKey,
                tx: hex::encode(&t.tx_id),
                chain: t.chain.clone(),
            }],
            RawPollRequests::Token(t) => vec![PollRequest {
                kind: PollKind::Token,
                tx: hex::encode(&t.tx_id),
                chain: t.chain.clone(),
            }],
        })
        .collect();

    let mut poll_creations = Vec::new();
    for request in poll_requests {
        if let Some(params) = chain_params.get(&request.chain.to_lowercase()) {
            let revote_period = params.revote_locking_period as u64;
            let expiry_height = height + revote_period;
            poll_creations.push(request.with_expiry(expiry_height));
        } else {
            log::warn!(
                "Dropping poll creation for chain '{}' at height {}: no chain params loaded",
                request.chain,
                height
            );
        }
    }

    let votes = get_votes_from_txs(&txs);

    Ok(RawBlockData {
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

    fn create_test_chain_params() -> BTreeMap<String, ChainParams> {
        let mut params = BTreeMap::new();
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

        let poll_data = &result.poll_creations[0];
        assert_eq!(
            poll_data.tx,
            "4af800f430dc829f3f08dd698dccdb3aac37438288653503bb2710f6cab386ec"
        );
        assert_eq!(poll_data.chain, "Avalanche");
        assert_eq!(poll_data.expiry_height, 20383480 + 15);

        match &poll_data.kind {
            crate::polls::PollKind::Deposit { burner_address } => {
                assert_eq!(burner_address, "64db450dae5f15853b9119918cd7dd7944e67510");
            }
            _ => panic!("Expected Deposit poll kind"),
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
            .find(|pc| matches!(pc.kind, crate::polls::PollKind::TransferKey))
            .expect("Should have TransferKey poll creation");

        assert_eq!(
            transfer_key.tx,
            "78e2698855ffb323320c8d4ae1dc85eb3c8e10b3a75180a7aadb238f769f6e8d"
        );
        assert_eq!(transfer_key.chain, "scroll");
        assert_eq!(transfer_key.expiry_height, 20404088 + 15);

        let gateway_tx = result
            .poll_creations
            .iter()
            .find(|pc| matches!(pc.kind, crate::polls::PollKind::GatewayTx))
            .expect("Should have GatewayTx poll creation");

        assert_eq!(
            gateway_tx.tx,
            "05a409afd25c53a59f98a48721a5274a450b4ac5a2007842b4e168a111af806e"
        );
        assert_eq!(gateway_tx.chain, "scroll");
        assert_eq!(gateway_tx.expiry_height, 20404088 + 15);
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
            matches!(pc.kind, crate::polls::PollKind::GatewayTx) && pc.chain == "binance"
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

        let poll_data = &result.poll_creations[0];
        assert!(matches!(poll_data.kind, crate::polls::PollKind::Token));
        assert_eq!(
            poll_data.tx,
            "05f9266335faf6ff82f98687f7f19398426faf3b1cca5ef26b235b42baa593e5"
        );
        assert_eq!(poll_data.chain, "Ethereum");
        assert_eq!(poll_data.expiry_height, 20133866 + 15);
    }
}
