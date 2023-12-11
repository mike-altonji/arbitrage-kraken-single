use crate::structs::{OrderData, OrderMap};
use crate::telegram::send_telegram_message;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::{tungstenite::Error::Protocol, MaybeTlsStream, WebSocketStream};

pub async fn fetch_orders(
    token: &String,
    orders: &Arc<Mutex<OrderMap>>,
) -> Result<(), Box<dyn std::error::Error>> {
    const SLEEP_DURATION: Duration = Duration::from_secs(5);
    loop {
        let (_write, mut read) = setup_private_websocket(token).await;
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    handle_message_text(&text, &orders).await?;
                }
                Ok(_) => {
                    let msg = "Private websocket connection closed or stopped sending data";
                    log::warn!("{}", msg);
                    send_telegram_message(msg).await;
                    tokio::time::sleep(SLEEP_DURATION).await;
                    continue;
                }
                Err(e) => {
                    match e {
                        Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                            log::warn!(
                                "Private server closed connection without a closing handshake. {:?}",
                                e
                            );
                        }
                        _ => {
                            let msg =
                                format!("Error during private websocket communication: {:?}", e);
                            log::error!("{}", msg);
                            send_telegram_message(&msg).await;
                        }
                    }
                    tokio::time::sleep(SLEEP_DURATION).await;
                    continue;
                }
            }
        }
    }
}

async fn setup_private_websocket(
    token: &String,
) -> (
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    let url = url::Url::parse("wss://ws-auth.kraken.com").expect("Private ws unparseable");
    let (ws, _) = connect_async(url)
        .await
        .expect("Failed to connect to private websocket");
    let (mut write, read) = ws.split();
    let sub_msg = serde_json::json!({
        "event": "subscribe",
        "subscription": {
            "name": "openOrders",
            "token": token
        }
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .expect("Failed to send private subscription message");
    log::info!("Subscribed to openOrders");

    (write, read)
}

async fn handle_message_text(
    text: &str,
    orders: &Arc<Mutex<OrderMap>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data: Value = serde_json::from_str(text)?;

    // Remove old orders from the `orders` map, to keep speed fast (once per heartbeat)
    if let Value::Object(map) = &data {
        if let Some(event) = map.get("event") {
            if event == "heartbeat" {
                let mut orders = orders.lock().unwrap();
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as f64;
                orders.retain(|_, order_data| now - order_data.lastupdated <= 600.0);
                return Ok(());
            }
        }
    }

    // Add new orders to `orders`
    if let Value::Array(main_array) = &data {
        if main_array.len() > 1 && main_array[1] == "openOrders" {
            if let Value::Array(order_array) = &main_array[0] {
                for order in order_array {
                    if let Value::Object(order_obj) = order {
                        for (_key, value) in order_obj {
                            if let Value::Object(order_data) = value {
                                if let Some(status) =
                                    order_data.get("status").and_then(|v| v.as_str())
                                {
                                    if status == "closed" || status == "cancelled" {
                                        let userref =
                                            order_data["userref"].as_i64().unwrap() as i32;
                                        let lastupdated = order_data["lastupdated"]
                                            .as_str()
                                            .unwrap()
                                            .parse::<f64>()
                                            .unwrap();
                                        let vol = order_data["vol_exec"]
                                            .as_str()
                                            .unwrap()
                                            .parse::<f64>()
                                            .unwrap();
                                        let cost = order_data["cost"]
                                            .as_str()
                                            .unwrap()
                                            .parse::<f64>()
                                            .unwrap();
                                        let fee = order_data["fee"]
                                            .as_str()
                                            .unwrap()
                                            .parse::<f64>()
                                            .unwrap();
                                        let price = order_data["avg_price"]
                                            .as_str()
                                            .unwrap()
                                            .parse::<f64>()
                                            .unwrap();

                                        let order_data = OrderData {
                                            lastupdated,
                                            vol,
                                            cost,
                                            fee,
                                            price,
                                        };

                                        let mut orders = orders.lock().unwrap();
                                        orders.insert(userref, order_data);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
