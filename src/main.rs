use axelar_monitor::{
    Config, IoCommand, ProcessingMessage, ProcessingResponse, ProcessingState,
    process_single_io_command, process_single_message,
};
use log::info;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod metrics;

fn io_thread_loop(
    rpc_url: String,
    lcd_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<ProcessingMessage>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(cmd) => {
                for msg in process_single_io_command(cmd, &rpc_url, &lcd_url) {
                    msg_tx.send(msg).unwrap();
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
        let msg = match msg_rx.recv() {
            Ok(msg) => msg,
            Err(_) => break,
        };
        for response in process_single_message(msg, &mut state, &config) {
            match response {
                ProcessingResponse::SendIoCommand(cmd) => {
                    cmd_tx.send(cmd).unwrap();
                }
                ProcessingResponse::SendMetricsSnapshot(response_tx, snapshot) => {
                    let _ = response_tx.send(snapshot);
                }
            }
        }
    }
    info!("processing loop done");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_level: String = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let log_level = log_level
        .parse()
        .unwrap_or_else(|e| panic!("failed to parse log level '{}': {}", log_level, e));

    simple_logger::init_with_level(log_level).unwrap();
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

    thread::spawn(move || {
        feeder_tx.send(IoCommand::FetchHead).unwrap();
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
