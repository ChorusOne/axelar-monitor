use axelar_watch::{
    Config, Height, IoCommand, IoResponse, ProcessingMessage, ProcessingResponse, ProcessingState,
    process_single_message,
};
use log::{error, info};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod metrics;

use axelar_watch::IoResult;
use axelar_watch::rpc::{get_block, get_block_results, get_chain_list, get_chain_params, get_head};

fn process_single_io_command(cmd: IoCommand, rpc_url: &str, lcd_url: &str) -> Vec<IoResponse> {
    match cmd {
        IoCommand::FetchBlock(height) => match get_block(rpc_url, height) {
            Ok((block, height)) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::Block(height, block),
            ))],
            Err(e) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::FetchError(height, e.to_string()),
            ))],
        },
        IoCommand::FetchBlockResults(height) => match get_block_results(lcd_url, height) {
            Ok(block_results) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::BlockResults(height, block_results),
            ))],
            Err(e) => {
                error!("Failed to fetch block results for height {}: {}", height, e);
                vec![]
            }
        },
        IoCommand::FetchChainList => match get_chain_list(rpc_url) {
            Ok(chains) => {
                info!("fetched chain list: {} chains", chains.len());
                vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                    IoResult::ChainList(chains),
                ))]
            }
            Err(e) => {
                error!("Failed to fetch chain list: {}", e);
                vec![]
            }
        },
        IoCommand::FetchHead => match get_head(rpc_url) {
            Ok(height) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::Head(height),
            ))],
            Err(e) => {
                error!("Failed to fetch chain head: {}", e);
                vec![]
            }
        },
        IoCommand::Shutdown => vec![IoResponse::Shutdown],
        IoCommand::FetchChainParams(chain) => match get_chain_params(rpc_url, &chain) {
            Ok(params) => vec![IoResponse::SendMessage(ProcessingMessage::IoResult(
                IoResult::ChainParams(params.into()),
            ))],
            Err(e) => {
                error!("Failed to fetch params for chain {}: {}", &chain, e);
                vec![]
            }
        },
    }
}

fn io_thread_loop(
    rpc_url: String,
    lcd_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<ProcessingMessage>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(cmd) => {
                let responses = process_single_io_command(cmd, &rpc_url, &lcd_url);
                let mut should_shutdown = false;

                for response in responses {
                    match response {
                        IoResponse::SendMessage(msg) => {
                            msg_tx.send(msg).unwrap();
                        }
                        IoResponse::Shutdown => {
                            msg_tx.send(ProcessingMessage::Shutdown).unwrap();
                            should_shutdown = true;
                        }
                    }
                }

                if should_shutdown {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    info!("exiting io thread loop");
}

fn processing_loop(
    msg_rx: mpsc::Receiver<ProcessingMessage>,
    cmd_tx: mpsc::Sender<IoCommand>,
    config: Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = ProcessingState::new(&config);

    loop {
        match msg_rx.recv()? {
            msg => {
                let responses = process_single_message(msg, &mut state, &config);
                let mut should_shutdown = false;

                for response in responses {
                    match response {
                        ProcessingResponse::SendIoCommand(cmd) => {
                            cmd_tx.send(cmd).unwrap();
                        }
                        ProcessingResponse::SendMetricsSnapshot(response_tx, snapshot) => {
                            let _ = response_tx.send(snapshot);
                        }
                        ProcessingResponse::Shutdown => {
                            should_shutdown = true;
                        }
                    }
                }

                if should_shutdown {
                    break;
                }
            }
        }
    }
    info!("processing loop done");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let config = Config::load("config.toml")?;

    let (cmd_tx, cmd_rx) = mpsc::channel::<IoCommand>();
    let (msg_tx, msg_rx) = mpsc::channel::<ProcessingMessage>();

    let rpc_url = config.rpc_url.clone();
    let lcd_url = config.lcd_url.clone();
    let poll_interval = config.poll_interval_seconds;
    let metrics_port = config.metrics_port;

    let msg_tx_io = msg_tx.clone();
    thread::spawn(move || {
        io_thread_loop(rpc_url, lcd_url, cmd_rx, msg_tx_io);
    });

    let feeder_tx = cmd_tx.clone();

    cmd_tx.send(IoCommand::FetchChainList).unwrap();

    let args: Vec<String> = std::env::args().collect();
    let single_block = if args.len() > 1 {
        args[1].parse::<u64>().ok()
    } else {
        None
    };

    thread::spawn(move || {
        // start at specific height
        feeder_tx.send(IoCommand::FetchHead).unwrap();
        if let Some(height) = single_block {
            println!("Starting to fetch from height {height}");
            feeder_tx
                .send(IoCommand::FetchBlock(Height::Specific(height)))
                .unwrap();
        }
        loop {
            thread::sleep(Duration::from_secs(poll_interval));
            feeder_tx.send(IoCommand::FetchHead).unwrap();
        }
    });
    let msg_tx_metrics = msg_tx.clone();
    thread::spawn(move || {
        metrics::metrics_server_loop(msg_tx_metrics, metrics_port);
    });

    processing_loop(msg_rx, cmd_tx, config)
}
