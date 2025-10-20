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

                metrics.push_str("# HELP last_processed_height Last block height that was processed\n");
                metrics.push_str("# TYPE last_processed_height gauge\n");
                metrics.push_str(&format!("last_processed_height {}\n", snapshot.last_processed_height));

                metrics.push_str("# HELP fetch_errors_total Total number of fetch errors\n");
                metrics.push_str("# TYPE fetch_errors_total counter\n");
                metrics.push_str(&format!(
                    "fetch_errors_total {}\n",
                    snapshot.fetch_error_count
                ));

                let mut sorted_stats: Vec<_> = snapshot.broadcaster_stats.iter().collect();
                sorted_stats.sort_by(|a, b| {
                    let ((broadcaster_a, chain_a), _) = a;
                    let ((broadcaster_b, chain_b), _) = b;
                    chain_a.cmp(chain_b).then_with(|| broadcaster_a.cmp(broadcaster_b))
                });

                metrics.push_str("# HELP broadcaster_votes_total Total number of votes cast by broadcaster on a chain\n");
                metrics.push_str("# TYPE broadcaster_votes_total counter\n");
                for ((broadcaster, chain), stats) in &sorted_stats {
                    metrics.push_str(&format!(
                        "broadcaster_votes_total{{chain=\"{}\",broadcaster=\"{}\"}} {}\n",
                        chain, broadcaster, stats.total_votes
                    ));
                }

                metrics.push_str("# HELP broadcaster_votes_disagreed Total number of votes where broadcaster disagreed with majority\n");
                metrics.push_str("# TYPE broadcaster_votes_disagreed counter\n");
                for ((broadcaster, chain), stats) in &sorted_stats {
                    metrics.push_str(&format!(
                        "broadcaster_votes_disagreed{{chain=\"{}\",broadcaster=\"{}\"}} {}\n",
                        chain, broadcaster, stats.disagreed_with_majority
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
