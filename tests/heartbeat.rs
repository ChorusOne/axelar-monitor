pub mod common;

use axelar_monitor::{
    Height, IoCommand, IoResponse, ProcessingResponse, ProcessingState, config,
    process_single_message,
};

#[test]
fn test_heartbeat_tracking() {
    let config = common::create_test_config_with_broadcasters(vec![
        config::Broadcaster {
            name: "chorus".to_string(),
            address: "be932e6de9f924116df2afc1829506ac853956e3".to_string(),
        },
        config::Broadcaster {
            name: "Ledger".to_string(),
            address: "653bec2260410227cea886d1f3277035869d39ae".to_string(),
        },
    ]);
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

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height)),
        "heartbeat",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = common::mock_process_io_command(cmd, "heartbeat", height);

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
    let config = common::create_test_config_with_broadcasters(vec![
        config::Broadcaster {
            name: "chorus".to_string(),
            address: "be932e6de9f924116df2afc1829506ac853956e3".to_string(),
        },
        config::Broadcaster {
            name: "Ledger".to_string(),
            address: "653bec2260410227cea886d1f3277035869d39ae".to_string(),
        },
    ]);
    let mut state = ProcessingState::new(&config);
    let height = 20432801;

    let io_responses = common::mock_process_io_command(
        IoCommand::FetchBlock(Height::Specific(height)),
        "heartbeat",
        height,
    );

    for io_resp in io_responses {
        if let IoResponse::SendMessage(msg) = io_resp {
            let proc_responses = process_single_message(msg, &mut state, &config);

            for proc_resp in proc_responses {
                if let ProcessingResponse::SendIoCommand(cmd) = proc_resp {
                    let io_responses2 = common::mock_process_io_command(cmd, "heartbeat", height);

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
