use crate::ProcessingMessage;
use std::{sync::mpsc, time::Duration};

pub fn metrics_server_loop(msg_tx: mpsc::Sender<ProcessingMessage>, port: u16, namespace: String) {
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

                metrics.push_str(&format!("# HELP {}_heartbeat_last_height Last height at which broadcaster sent heartbeat\n", namespace));
                metrics.push_str(&format!("# TYPE {}_heartbeat_last_height gauge\n", namespace));
                for (name, height) in &snapshot.last_heartbeat {
                    metrics.push_str(&format!(
                        "{}_heartbeat_last_height{{broadcaster=\"{}\"}} {}\n",
                        namespace, name, height
                    ));
                }

                metrics.push_str(&format!("# HELP {}_chain_height Current chain height\n", namespace));
                metrics.push_str(&format!("# TYPE {}_chain_height gauge\n", namespace));
                metrics.push_str(&format!("{}_chain_height {}\n", namespace, snapshot.chain_height));

                metrics.push_str(&format!("# HELP {}_last_processed_height Last block height that was processed\n", namespace));
                metrics.push_str(&format!("# TYPE {}_last_processed_height gauge\n", namespace));
                metrics.push_str(&format!("{}_last_processed_height {}\n", namespace, snapshot.last_processed_height));

                metrics.push_str(&format!("# HELP {}_fetch_errors_total Total number of fetch errors\n", namespace));
                metrics.push_str(&format!("# TYPE {}_fetch_errors_total counter\n", namespace));
                metrics.push_str(&format!(
                    "{}_fetch_errors_total {}\n",
                    namespace, snapshot.fetch_error_count
                ));

                let mut sorted_stats: Vec<_> = snapshot.broadcaster_stats.iter().collect();
                sorted_stats.sort_by(|a, b| {
                    let ((broadcaster_a, chain_a), _) = a;
                    let ((broadcaster_b, chain_b), _) = b;
                    chain_a.cmp(chain_b).then_with(|| broadcaster_a.cmp(broadcaster_b))
                });

                metrics.push_str(&format!("# HELP {}_broadcaster_votes_total Total number of votes cast by broadcaster on a chain\n", namespace));
                metrics.push_str(&format!("# TYPE {}_broadcaster_votes_total counter\n", namespace));
                for ((broadcaster, chain), stats) in &sorted_stats {
                    metrics.push_str(&format!(
                        "{}_broadcaster_votes_total{{chain=\"{}\",broadcaster=\"{}\"}} {}\n",
                        namespace, chain, broadcaster, stats.total_votes
                    ));
                }

                metrics.push_str(&format!("# HELP {}_broadcaster_votes_disagreed Total number of votes where broadcaster disagreed with majority\n", namespace));
                metrics.push_str(&format!("# TYPE {}_broadcaster_votes_disagreed counter\n", namespace));
                for ((broadcaster, chain), stats) in &sorted_stats {
                    metrics.push_str(&format!(
                        "{}_broadcaster_votes_disagreed{{chain=\"{}\",broadcaster=\"{}\"}} {}\n",
                        namespace, chain, broadcaster, stats.disagreed_with_majority
                    ));
                }

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
