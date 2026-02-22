use axelar_monitor::{
    Config, IoCommand, IoResult, ProcessingState, process_single_io_command, process_single_message,
};
use log::info;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

mod metrics;

fn io_thread_loop(
    rpc_url: String,
    lcd_url: String,
    cmd_rx: mpsc::Receiver<IoCommand>,
    msg_tx: mpsc::Sender<IoResult>,
) {
    loop {
        match cmd_rx.recv() {
            Ok(cmd) => {
                for result in process_single_io_command(cmd, &rpc_url, &lcd_url) {
                    msg_tx.send(result).unwrap();
                }
            }
            Err(_) => break,
        }
    }
    info!("exiting io thread loop");
}

fn processing_loop(
    msg_rx: mpsc::Receiver<IoResult>,
    cmd_tx: mpsc::Sender<IoCommand>,
    config: Config,
    metrics: Arc<Mutex<axelar_monitor::MetricsSnapshot>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = ProcessingState::new(&config);

    loop {
        let result = match msg_rx.recv() {
            Ok(result) => result,
            Err(_) => break,
        };
        for cmd in process_single_message(result, &mut state, &config) {
            cmd_tx.send(cmd).unwrap();
        }
        *metrics.lock().unwrap() = state.metrics_snapshot();
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
    let (msg_tx, msg_rx) = mpsc::channel::<IoResult>();

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

    let state = ProcessingState::new(&config);
    let metrics = Arc::new(Mutex::new(state.metrics_snapshot()));
    let metrics_clone = metrics.clone();
    thread::spawn(move || {
        metrics::metrics_server_loop(metrics_clone, metrics_port);
    });

    processing_loop(msg_rx, cmd_tx, config, metrics)
}
