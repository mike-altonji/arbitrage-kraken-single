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
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

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

