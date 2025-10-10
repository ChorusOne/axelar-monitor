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
            // getting an answer should take <1ms
            if let Ok(snapshot) = response_rx.recv_timeout(Duration::from_secs(1)) {
                let mut metrics = String::new();

                metrics.push_str("# HELP heartbeat_last_height Last height at which broadcaster sent heartbeat\n");
                metrics.push_str("# TYPE heartbeat_last_height gauge\n");
                for (name, height) in &snapshot.last_heartbeat {
                    metrics.push_str(&format!(
                        "heartbeat_last_height{{broadcaster=\"{}\"}} {}\n",
                        name, height
                    ));
                }

                metrics.push_str("# HELP chain_height Current chain height\n");
                metrics.push_str("# TYPE chain_height gauge\n");
                metrics.push_str(&format!("chain_height {}\n", snapshot.chain_height));

                metrics.push_str("# HELP fetch_errors_total Total number of fetch errors\n");
                metrics.push_str("# TYPE fetch_errors_total counter\n");
                metrics.push_str(&format!(
                    "fetch_errors_total {}\n",
                    snapshot.fetch_error_count
                ));

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
