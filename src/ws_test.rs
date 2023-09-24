use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use log::{info, LevelFilter};
use env_logger::Builder;
use serde_json::json;

#[tokio::main]
async fn main() {
    setup_logger();

    // Connect to WebSocket API and subscribe to public trade feed
    let (mut ws, _) = connect_async("wss://ws.kraken.com/").await.expect("Error connecting to the WebSocket");

    let subscription_message = json!({
        "event": "subscribe",
        "subscription": {"name": "trade"},
        "pair": ["XBT/CAD"]
    });
    ws.send(Message::Text(subscription_message.to_string())).await.expect("Failed to send a message");

    // Infinite loop waiting for WebSocket data
    let t0 = tokio::time::Instant::now();
    while tokio::time::Instant::now().duration_since(t0) < Duration::from_secs(120) {
        match ws.next().await {
            Some(Ok(Message::Text(resp))) => {
                let current_time = tokio::time::Instant::now();
                info!("{} | {:?}", resp, current_time);
            }
            Some(Err(e)) => {
                println!("Error while receiving a message: {:?}", e);
            }
            _ => {}
        }
    }

    ws.close(None).await.expect("Error closing the WebSocket");
}

fn setup_logger() {
    Builder::new()
        .format(|buf, record| writeln!(buf, "{} [{}] - {}", record.target(), record.level(), record.args()))
        .filter(None, LevelFilter::Info)
        .write_style(None)
        .init();
}
