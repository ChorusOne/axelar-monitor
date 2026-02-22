pub mod common;

use axelar_monitor::{
    Height, IoCommand, IoResponse, ProcessingResponse, ProcessingState, polls,
    process_single_message,
};

#[test]
fn test_full_poll_flow_deposit() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20383480;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "deposit",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            assert!(
                proc_responses.iter().any(|r| matches!(
                    r,
                    ProcessingResponse::SendIoCommand(IoCommand::FetchBlockResults(..))
                )),
                "Should request BlockResults"
            );

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = common::mock_process_io_command(cmd, "deposit", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 1, "Should have created 1 poll");
    assert!(
        state.polls.contains_key(&2839857),
        "Should contain poll_id 2839857"
    );

    let poll = &state.polls[&2839857];
    assert_eq!(
        poll.data.tx,
        "4af800f430dc829f3f08dd698dccdb3aac37438288653503bb2710f6cab386ec"
    );
    assert_eq!(poll.data.chain, "Avalanche");
    match &poll.data.kind {
        polls::PollKind::Deposit { burner_address } => {
            assert_eq!(burner_address, "64db450dae5f15853b9119918cd7dd7944e67510");
        }
        _ => panic!("Expected Deposit poll kind"),
    }
}

#[test]
fn test_full_poll_flow_transfer_key() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20404088;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "transfer_key",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 =
                        common::mock_process_io_command(cmd, "transfer_key", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 2, "Should have created 2 polls");
    assert!(
        state.polls.contains_key(&2843160),
        "Should contain TransferKey poll_id 2843160"
    );
    assert!(
        state.polls.contains_key(&2843161),
        "Should contain GatewayTx poll_id 2843161"
    );

    let transfer_key_poll = &state.polls[&2843160];
    assert!(matches!(
        transfer_key_poll.data.kind,
        polls::PollKind::TransferKey
    ));
    assert_eq!(
        transfer_key_poll.data.tx,
        "78e2698855ffb323320c8d4ae1dc85eb3c8e10b3a75180a7aadb238f769f6e8d"
    );
    assert_eq!(transfer_key_poll.data.chain, "scroll");

    let gateway_tx_poll = &state.polls[&2843161];
    assert!(matches!(
        gateway_tx_poll.data.kind,
        polls::PollKind::GatewayTx
    ));
    assert_eq!(
        gateway_tx_poll.data.tx,
        "05a409afd25c53a59f98a48721a5274a450b4ac5a2007842b4e168a111af806e"
    );
    assert_eq!(gateway_tx_poll.data.chain, "scroll");
}

#[test]
fn test_full_poll_flow_gateway_txs_batch() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20413624;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "gateway_txs",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = common::mock_process_io_command(cmd, "gateway_txs", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert!(
        state.polls.len() >= 1,
        "Should have created at least 1 poll from batch"
    );

    let has_binance_poll = state.polls.values().any(|poll| {
        matches!(poll.data.kind, polls::PollKind::GatewayTx) && poll.data.chain == "binance"
    });

    assert!(
        has_binance_poll,
        "Should have created a binance GatewayTx poll"
    );
}

#[test]
fn test_full_poll_flow_confirm_token() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20133866;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "confirm_token",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            assert!(
                proc_responses.iter().any(|r| matches!(
                    r,
                    ProcessingResponse::SendIoCommand(IoCommand::FetchBlockResults(..))
                )),
                "Should request BlockResults"
            );

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 =
                        common::mock_process_io_command(cmd, "confirm_token", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 1, "Should have created 1 poll");
    assert!(
        state.polls.contains_key(&2803037),
        "Should contain poll_id 2803037"
    );

    let poll = &state.polls[&2803037];
    assert!(matches!(poll.data.kind, polls::PollKind::Token));
    assert_eq!(
        poll.data.tx,
        "05f9266335faf6ff82f98687f7f19398426faf3b1cca5ef26b235b42baa593e5"
    );
    assert_eq!(poll.data.chain, "Ethereum");
}

#[test]
fn test_full_poll_flow_confirm_gateway_tx_started() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20414583;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "confirm_gateway_tx",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            assert!(
                proc_responses.iter().any(|r| matches!(
                    r,
                    ProcessingResponse::SendIoCommand(IoCommand::FetchBlockResults(..))
                )),
                "Should request BlockResults"
            );

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 =
                        common::mock_process_io_command(cmd, "confirm_gateway_tx", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 1, "Should have created 1 poll");
    assert!(
        state.polls.contains_key(&2846687),
        "Should contain poll_id 2846687"
    );

    let poll = &state.polls[&2846687];
    assert!(matches!(poll.data.kind, polls::PollKind::GatewayTx));
    assert_eq!(
        poll.data.tx,
        "3016e9691a809ae405ffc764fc9fe09eb09535b1806e688067527a9e38c979a0"
    );
    assert_eq!(poll.data.chain, "blast");
}

#[test]
fn test_new_protocol_plaintext_events() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 24678401;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "new_protocol",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            assert!(
                proc_responses.iter().any(|r| matches!(
                    r,
                    ProcessingResponse::SendIoCommand(IoCommand::FetchBlockResults(..))
                )),
                "Should request BlockResults for poll creation"
            );

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 =
                        common::mock_process_io_command(cmd, "new_protocol", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 1, "Should have created 1 poll");
    assert!(
        state.polls.contains_key(&3045599),
        "Should contain poll_id 3045599"
    );

    let poll = &state.polls[&3045599];
    assert!(matches!(poll.data.kind, polls::PollKind::GatewayTx));
    assert_eq!(
        poll.data.tx,
        "4f68a703e0bba0a0f6fd9d484d8b3674584c6ca54eef68ff4e733f3774bbd814"
    );
    assert_eq!(poll.data.chain, "polygon");

    let vote_height = 24678402;
    let vote_io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(vote_height), 0),
        "new_protocol",
        vote_height,
    );

    for io_resp in vote_io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            process_single_message(msg, &mut state, &config);
        }
    }

    let poll = &state.polls[&3045599];
    assert!(
        !poll.votes.is_empty(),
        "Poll 3045599 should have received votes from block 24678402"
    );
}

#[test]
fn test_new_protocol_bech32_sender_matches_config() {
    let config =
        common::create_test_config_with_broadcasters(vec![axelar_monitor::config::Broadcaster {
            name: "chorus".to_string(),
            address: "be932e6de9f924116df2afc1829506ac853956e3".to_string(),
        }]);
    let mut state = ProcessingState::new(&config);

    // Block 24678401: creates poll 3045599 on Polygon
    let height = 24678401;
    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "new_protocol",
        height,
    );
    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);
            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    for io_resp2 in common::mock_process_io_command(cmd, "new_protocol", height) {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }
    assert!(state.polls.contains_key(&3045599));

    // Block 24678404: chorus vote on poll 3045599
    // chorus = axelar1h6fjum0flyjpzm0j4lqc99gx4jznj4hr8yj0g5
    let vote_height = 24678404;
    let vote_io = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(vote_height), 0),
        "new_protocol",
        vote_height,
    );
    for io_resp in vote_io {
        if let IoResponse::SendMessage(msg) = io_resp {
            process_single_message(msg, &mut state, &config);
        }
    }

    let poll = &state.polls[&3045599];
    let chorus_votes: Vec<_> = poll
        .votes
        .iter()
        .filter(|v| v.sender_id == "be932e6de9f924116df2afc1829506ac853956e3")
        .collect();
    assert!(
        !chorus_votes.is_empty(),
        "Chorus bech32 sender should decode to matching hex address"
    );
}

#[test]
fn test_empty_block_no_polls_no_votes() {
    let config = common::create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20432569;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height), 0),
        "empty_block",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = common::mock_process_io_command(cmd, "empty_block", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(state.polls.len(), 0, "Should have created 0 polls");
}
