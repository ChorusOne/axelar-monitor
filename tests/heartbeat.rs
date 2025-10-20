use axelar_watch::{
    Config, Height, IoCommand, IoResponse, ProcessingMessage, ProcessingResponse, ProcessingState,
    blocks, config, process_single_message, rpc,
};

fn load_test_block_from_json(test_type: &str, height: u64) -> blocks::Block {
    let path = format!("test_data/{}/block_{}.json", test_type, height);
    let json = std::fs::read_to_string(&path).unwrap();
    blocks::parse_block(&json).unwrap()
}

fn load_test_block_results_from_json(test_type: &str, height: u64) -> rpc::BlockResults {
    let path = format!("test_data/{}/block_result_{}.json", test_type, height);
    let json = std::fs::read_to_string(&path).unwrap();
    let response: rpc::BlockResultsResponse = serde_json::from_str(&json).unwrap();
    response.result
}

fn mock_process_io_command(cmd: IoCommand, test_type: &str, _height: u64) -> Vec<IoResponse> {
    match cmd {
        IoCommand::FetchBlock(Height::Specific(h)) => {
            let block = load_test_block_from_json(test_type, h);
            vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                axelar_watch::IoResult::Block(h, block),
            ))]
        }
        IoCommand::FetchBlockResults(h) => {
            let block_results = load_test_block_results_from_json(test_type, h);
            vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                axelar_watch::IoResult::BlockResults(h, block_results),
            ))]
        }
        _ => vec![],
    }
}

fn create_test_config() -> Config {
    Config {
        rpc_url: "http://test".to_string(),
        lcd_url: "http://test".to_string(),
        poll_interval_seconds: 6,
        metrics_port: 9090,
        broadcaster: vec![
            config::Broadcaster {
                name: "chorus".to_string(),
                address: "be932e6de9f924116df2afc1829506ac853956e3".to_string(),
            },
            config::Broadcaster {
                name: "Ledger".to_string(),
                address: "653bec2260410227cea886d1f3277035869d39ae".to_string(),
            },
        ],
        chain_params: vec![],
    }
}

#[test]
fn test_heartbeat_tracking() {
    let config = create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20432801;

    assert_eq!(
        state.last_heartbeat.get("chorus"),
        Some(&0),
        "chorus should start with heartbeat at 0"
    );
    assert_eq!(
        state.last_heartbeat.get("Ledger"),
        Some(&0),
        "Ledger should start with heartbeat at 0"
    );

    let io_responses = mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height)),
        "heartbeat",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = mock_process_io_command(cmd, "heartbeat", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(
        state.last_heartbeat.get("chorus"),
        Some(&height),
        "chorus heartbeat should be updated to block height"
    );
    assert_eq!(
        state.last_heartbeat.get("Ledger"),
        Some(&height),
        "Ledger heartbeat should be updated to block height"
    );

    assert_eq!(state.polls.len(), 0, "Should have created 0 polls");
}

#[test]
fn test_heartbeat_missing_broadcaster() {
    let config = create_test_config();
    let mut state = ProcessingState::new(&config);
    let height = 20432801;

    let io_responses = mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height)),
        "heartbeat",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = mock_process_io_command(cmd, "heartbeat", height);

                    for io_resp2 in io_responses2 {
                        if let IoResponse::SendMessage(msg2) = io_resp2 {
                            process_single_message(msg2, &mut state, &config);
                        }
                    }
                }
            }
        }
    }

    assert_eq!(
        state.last_heartbeat.get("unknown_broadcaster"),
        None,
        "Unknown broadcaster should not be tracked"
    );
}
