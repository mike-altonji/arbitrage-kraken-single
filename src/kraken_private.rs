use base64::{decode_config, encode_config, STANDARD};
use futures_util::SinkExt;
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use futures_util::StreamExt; // Add this import at the top of your file

pub async fn get_auth_token() -> Result<String, Box<dyn std::error::Error>> {
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");
    let api_path = "/0/private/GetWebSocketsToken";
    let api_nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let api_post = format!("nonce={}", api_nonce);

    let api_nonce_bytes = api_nonce.as_bytes();
    let api_post_bytes = api_post.as_bytes();

    let mut hasher = Sha256::new();
    hasher.update(api_nonce_bytes);
    hasher.update(api_post_bytes);
    let api_sha256 = hasher.finalize();
    let api_secret_decoded = decode_config(&api_secret, STANDARD).unwrap();
    let mut mac = Hmac::<Sha512>::new_varkey(&api_secret_decoded).unwrap();
    mac.update(api_path.as_bytes());
    mac.update(&api_sha256);
    let api_hmac = mac.finalize();
    let api_signature = encode_config(&api_hmac.into_bytes(), STANDARD);
    let mut headers = HeaderMap::new();
    headers.insert("API-Key", HeaderValue::from_str(&api_key.to_string())?);
    headers.insert(
        "API-Sign",
        HeaderValue::from_str(&api_signature.to_string())?,
    );

    let client = reqwest::Client::new();
    let res = client
        .post("https://api.kraken.com/0/private/GetWebSocketsToken")
        .headers(headers)
        .body(api_post)
        .send()
        .await?;

    let response_text = res.text().await?;
    let v: Value = serde_json::from_str(&response_text)?;
    let token = v["result"]["token"].as_str().unwrap().to_string();

    Ok(token)
}

// TODO: Handle reconnecting when errors or unsubscribed
pub async fn connect_to_private_feed(
    auth_token: Arc<Mutex<Option<String>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut ws_stream, _) = connect_async("wss://ws-auth.kraken.com").await?;

    let token = get_auth_token().await?;

    {
        let mut locked_token = auth_token.lock().unwrap();
        *locked_token = Some(token);
    } // MutexGuard for locked_token is dropped here

    let token_clone = {
        let locked_token = auth_token.lock().unwrap();
        locked_token.clone().unwrap()
    }; // MutexGuard for locked_token is dropped immediately

    // Subscribe to ownTrades
    let subscription_message = serde_json::json!({
        "event": "subscribe",
        "subscription": {
            "name": "ownTrades",
            "token": token_clone.to_string()
        }
    });

    ws_stream
        .send(Message::Text(subscription_message.to_string()))
        .await?;

    // Print messages from the feed
    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(msg) => println!("Received: {}", msg),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}

pub async fn execute_trade(
    private_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    auth_token: Arc<Mutex<String>>,
    asset1: &str,
    asset2: &str,
    volume: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = auth_token.lock().unwrap().to_string();
    // TODO: Make buy or sell depending on base/quote, which means pair is dynamic
    let trade_message = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": "buy",
        "ordertype": "market",
        "volume": volume,
        "pair": format!("{}/{}", asset1, asset2),
    });
    private_stream
        .send(Message::Text(trade_message.to_string()))
        .await?;
    Ok(())
}
