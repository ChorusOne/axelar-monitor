use axelar_monitor::MetricsSnapshot;
use std::sync::{Arc, Mutex};

pub fn metrics_server_loop(snapshot: Arc<Mutex<MetricsSnapshot>>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);
    let server = tiny_http::Server::http(&addr).unwrap();
    println!("Metrics server listening on {}", addr);

    for request in server.incoming_requests() {
        if request.url() != "/metrics" {
            let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
            let _ = request.respond(response);
            continue;
        }
        let metrics = snapshot.lock().unwrap().to_string();
        let response = tiny_http::Response::from_string(metrics);
        let _ = request.respond(response);
    }
}
