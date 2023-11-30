use base64::{decode_config, encode_config, STANDARD};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::kraken::{AssetsToPair, Spread};

const FIAT_BALANCE: f64 = 1000.0; // TODO: Replace with real number, kept up-to-date

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

pub async fn execute_trade(
    path_names: Vec<String>,
    min_volume: f64,
    assets_to_pair: &HashMap<(String, String), AssetsToPair>,
    pair_to_spread: HashMap<String, Spread>,
    private_ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    token: &String,
    fee_pct: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut volume = min_volume.min(FIAT_BALANCE); // TODO: Remove hard-coding
    for i in 0..path_names.len() - 1 {
        let asset1 = &path_names[i];
        let asset2 = &path_names[i + 1];
        let pair_data = assets_to_pair
            .get(&(asset1.to_string(), asset2.to_string()))
            .ok_or(format!("Trade failed: No {} & {} pair", asset1, asset2))?;
        let pair = &pair_data.pair;
        let base = &pair_data.base;
        let (buy_sell, new_volume) =
            determine_trade_info(asset1, asset2, base, pair, volume, fee_pct, &pair_to_spread)?;
        volume = new_volume;
        let _ = make_trade(token, &buy_sell, volume, pair, private_ws);
        volume = process_trade_response(private_ws, pair, base, asset1, asset2).await?;
    }
    Ok(())
}

fn determine_trade_info(
    asset1: &String,
    asset2: &String,
    base: &String,
    pair: &String,
    volume: f64,
    fee_pct: f64,
    pair_to_spread: &HashMap<String, Spread>,
) -> Result<(String, f64), Box<dyn std::error::Error>> {
    let trade_type;
    let new_volume = if asset1 == base {
        trade_type = "sell".to_string();
        volume
    } else if asset2 == base {
        let ask = pair_to_spread[pair].ask;
        trade_type = "buy".to_string();
        (volume * (1. - fee_pct)) / ask
    } else {
        let msg = format!("Trade failed: Neither {} nor {} is base", asset1, asset2);
        return Err(msg.into());
    };
    Ok((trade_type, new_volume))
}

async fn make_trade(
    token: &String,
    trade_type: &String,
    volume: f64,
    pair: &String,
    private_ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let trade_msg = serde_json::json!({
        "event": "addOrder",
        "token": token,
        "type": trade_type,
        "ordertype": "market",
        "volume": volume,
        "pair": pair,
    })
    .to_string();
    private_ws.send(Message::Text(trade_msg)).await?;
    Ok(())
}

async fn process_trade_response(
    private_ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    pair: &String,
    base: &String,
    asset1: &String,
    asset2: &String,
) -> Result<f64, Box<dyn std::error::Error>> {
    let mut volume: Option<f64> = None;
    let timeout_duration = Duration::from_secs(10);
    let result = timeout(timeout_duration, async {
        while let Some(message) = private_ws.next().await {
            match message {
                Ok(msg) => {
                    let data: serde_json::Value = serde_json::from_str(&msg.to_string())?;
                    if let Some(trades) = data.get("ownTrades") {
                        volume = process_trades(trades, pair, base, asset1, asset2)?;
                        if volume.is_some() {
                            break;
                        }
                    }
                }
                Err(e) => log::error!("Message error on process_trade_response: {}", e),
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    match result {
        Ok(_) => volume.ok_or("Could not compute volume".into()),
        Err(_) => Err(format!("Timeout reached waiting for trade response of {}", pair).into()),
    }
}

fn process_trades(
    trades: &serde_json::Value,
    pair: &String,
    base: &String,
    asset1: &String,
    asset2: &String,
) -> Result<Option<f64>, Box<dyn std::error::Error>> {
    let trades_object = trades.as_object().ok_or("Err: trades_object")?;

    for trade in trades_object {
        let trade_data = trade.1.as_object().ok_or("Err: trade_data")?;

        let received_pair = trade_data
            .get("pair")
            .ok_or("Failed to get 'pair' from trade data")?
            .as_str()
            .ok_or("Failed to parse 'pair' as string")?;

        if received_pair == pair {
            let volume = calculate_volume(trade_data, base, asset1, asset2)?;
            if volume.is_some() {
                return Ok(volume);
            }
        }
    }
    Ok(None)
}

fn calculate_volume(
    trade_data: &serde_json::Map<String, serde_json::Value>,
    base: &String,
    asset1: &String,
    asset2: &String,
) -> Result<Option<f64>, Box<dyn std::error::Error>> {
    let cost = trade_data
        .get("cost")
        .ok_or("Failed to get 'cost' from trade data")?
        .as_str()
        .ok_or("Failed to parse 'cost' as string")?
        .parse::<f64>()
        .map_err(|_| "Failed to parse 'cost' as f64")?;

    let price = trade_data
        .get("price")
        .ok_or("Failed to get 'price' from trade data")?
        .as_str()
        .ok_or("Failed to parse 'price' as string")?
        .parse::<f64>()
        .map_err(|_| "Failed to parse 'price' as f64")?;

    let fee = trade_data
        .get("fee")
        .ok_or("Failed to get 'fee' from trade data")?
        .as_str()
        .ok_or("Failed to parse 'fee' as string")?
        .parse::<f64>()
        .map_err(|_| "Failed to parse 'fee' as f64")?;

    let volume = if asset1 == base {
        Some(cost - fee)
    } else if asset2 == base {
        Some((cost - fee) / price)
    } else {
        return Err(format!("Trade failed: Neither {} nor {} is base", asset1, asset2).into());
    };

    Ok(volume)
}
