use axelar_monitor::{
    Config, Height, IoCommand, IoResponse, ProcessingResponse, ProcessingState,
    process_single_io_command, process_single_message,
};

fn handle_request(request: &tiny_http::Request) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    let url_path = request.url().to_string();
    let json_header =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let latest = 20383480;

    let response = if url_path == "/cosmos/base/tendermint/v1beta1/blocks/latest" {
        let content =
            std::fs::read_to_string(format!("test_data/deposit/block_{latest}.json")).unwrap();
        Some(content)
    } else if url_path.starts_with("/cosmos/base/tendermint/v1beta1/blocks/") {
        let parts: Vec<&str> = url_path.rsplitn(2, '/').collect();
        let height_str = parts[0];

        let file_path = if height_str == "latest" {
            format!("test_data/deposit/block_{}.json", latest)
        } else {
            format!("test_data/deposit/block_{}.json", height_str)
        };

        std::fs::read_to_string(&file_path).ok()
    } else if url_path.starts_with("/block_results?height=") {
        let height_str = url_path.split("height=").nth(1).unwrap();
        let file_path = format!("test_data/deposit/block_result_{}.json", height_str);

        std::fs::read_to_string(&file_path).ok()
    } else if url_path == "/axelar/evm/v1beta1/chains" {
        let content = r#"{"chains":["Avalanche"]}"#;
        Some(content.to_string())
    } else if url_path == "/axelar/evm/v1beta1/params/Avalanche" {
        let content = r#"{"params":{"chain":"Avalanche","revote_locking_period":"15","voting_grace_period":"3"}}"#;
        Some(content.to_string())
    } else {
        None
    };

    match response {
        Some(content) => tiny_http::Response::from_string(content).with_header(json_header),
        None => tiny_http::Response::from_string("Not found").with_status_code(404),
    }
}

#[test]
fn test_with_http_server() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    let height = 20383480;
    let server =
        Arc::new(tiny_http::Server::http("127.0.0.1:0").expect("Failed to start test server"));
    let server_addr = server.server_addr().to_ip().unwrap();
    let base_url = format!("http://{}", server_addr);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let server_clone = server.clone();
    let server_thread = thread::spawn(move || {
        while !shutdown_clone.load(Ordering::Relaxed) {
            let request = match server_clone.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(req)) => req,
                Ok(None) => continue,
                Err(_) => continue,
            };

            let response = handle_request(&request);
            let _ = request.respond(response);
        }
    });

    let config = Config {
        rpc_url: base_url.clone(),
        lcd_url: base_url.clone(),
        poll_interval_seconds: 6,
        metrics_port: 9090,
        broadcaster: vec![],
        chain_params: vec![],
    };

    let mut state = ProcessingState::new(&config);

    // Fetch head
    let io_responses =
        process_single_io_command(IoCommand::FetchHead, &config.rpc_url, &config.lcd_url);
    for IoResponse::SendMessage(msg) in io_responses {
        process_single_message(msg, &mut state, &config);
    }
    assert_eq!(state.chain_tip, height, "Should have set chain tip");

    // Fetch chain list
    let io_responses =
        process_single_io_command(IoCommand::FetchChainList, &config.rpc_url, &config.lcd_url);
    for IoResponse::SendMessage(msg) in io_responses {
        let mut proc_responses = process_single_message(msg, &mut state, &config);

        assert_eq!(proc_responses.len(), 1);
        let resp = proc_responses.pop().unwrap();
        if let ProcessingResponse::SendIoCommand(cmd) = resp {
            let io_responses2 = process_single_io_command(cmd, &config.rpc_url, &config.lcd_url);
            for IoResponse::SendMessage(msg2) in io_responses2 {
                process_single_message(msg2, &mut state, &config);
            }
        }
    }
    assert_eq!(
        state.chain_params.len(),
        1,
        "Should have fetched 1 chain param"
    );
    assert!(
        state.chain_params.contains_key("avalanche"),
        "Should have Avalanche params"
    );

    // Fetch block though specific and /latest endpoints
    for h in vec![Height::Specific(height), Height::Latest] {
        let io_responses = process_single_io_command(
            IoCommand::FetchBlock(h, 0),
            &config.rpc_url,
            &config.lcd_url,
        );

        for IoResponse::SendMessage(msg) in io_responses {
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
                        process_single_io_command(cmd, &config.rpc_url, &config.lcd_url);

                    for IoResponse::SendMessage(msg2) in io_responses2 {
                        process_single_message(msg2, &mut state, &config);
                    }
                }
            }
        }

        assert_eq!(state.polls.len(), 1, "Should have created 1 poll");
        assert!(
            state.polls.contains_key(&2839857),
            "Should contain poll_id 2839857"
        );
    }

    shutdown.store(true, Ordering::Relaxed);
    let _ = server_thread.join();
}
