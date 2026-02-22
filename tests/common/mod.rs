use axelar_monitor::{
    Config, Height, IoCommand, IoResponse, ProcessingMessage, blocks, config, rpc,
};

pub fn load_test_block_from_json(test_type: &str, height: u64) -> blocks::Block {
    let path = format!("test_data/{}/block_{}.json", test_type, height);
    let json = std::fs::read_to_string(&path).unwrap();
    blocks::parse_block(&json).unwrap()
}

pub fn load_test_block_results_from_json(test_type: &str, height: u64) -> rpc::BlockResults {
    let path = format!("test_data/{}/block_result_{}.json", test_type, height);
    let json = std::fs::read_to_string(&path).unwrap();
    let response: rpc::BlockResultsResponse = serde_json::from_str(&json).unwrap();
    response.result
}

pub fn mock_process_io_command(cmd: IoCommand, test_type: &str, _height: u64) -> Vec<IoResponse> {
    match cmd {
        IoCommand::FetchBlock(Height::Specific(h), _) => {
            let block = load_test_block_from_json(test_type, h);
            vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                axelar_monitor::IoResult::Block(h, block),
            ))]
        }
        IoCommand::FetchBlockResults(h, poll_data, _) => {
            let block_results = load_test_block_results_from_json(test_type, h);
            vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                axelar_monitor::IoResult::BlockResults(h, block_results, poll_data),
            ))]
        }
        _ => vec![],
    }
}

pub fn create_test_config() -> Config {
    create_test_config_with_broadcasters(vec![])
}

pub fn create_test_config_with_broadcasters(broadcasters: Vec<config::Broadcaster>) -> Config {
    Config {
        rpc_url: "http://test".to_string(),
        lcd_url: "http://test".to_string(),
        poll_interval_seconds: 6,
        metrics_port: 9090,
        broadcaster: broadcasters,
        chain_params: vec![
            config::ChainParams {
                name: "Avalanche".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "scroll".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "binance".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "Ethereum".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "ethereum".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "blast".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
            config::ChainParams {
                name: "polygon".to_string(),
                revote_locking_period: 15,
                voting_grace_period: 3,
            },
        ],
    }
}
