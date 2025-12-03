use crate::ProcessingMessage;
use std::{sync::mpsc, time::Duration};

pub fn metrics_server_loop(msg_tx: mpsc::Sender<ProcessingMessage>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let server = tiny_http::Server::http(&addr).unwrap();
    println!("Metrics server listening on {}", addr);

    for request in server.incoming_requests() {
        if request.url() != "/metrics" {
            let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
            let _ = request.respond(response);
            continue;
        }
        let (response_tx, response_rx) = mpsc::channel();

        if msg_tx
            .send(ProcessingMessage::QueryMetrics(response_tx))
            .is_ok()
        {
            if let Ok(snapshot) = response_rx.recv_timeout(Duration::from_secs(1)) {
                let metrics = snapshot.to_string();
                let response = tiny_http::Response::from_string(metrics);
                let _ = request.respond(response);
            } else {
                let response = tiny_http::Response::from_string("Error querying metrics")
                    .with_status_code(500);
                let _ = request.respond(response);
            }
        } else {
            let response =
                tiny_http::Response::from_string("Error sending query").with_status_code(500);
            let _ = request.respond(response);
        }
    }
}
