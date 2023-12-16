use base64::{decode_config, encode_config, STANDARD};
use futures_util::SinkExt;
use hmac::{Hmac, Mac, NewMac};
use influx_db_client::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::structs::{AssetsToPair, OrderMap, PairToSpread};
use crate::trade::{trade_leg_to_influx, trade_path_to_influx};

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

    Ok(token.to_string())
}

pub async fn get_30d_trade_volume() -> Result<f64, Box<dyn std::error::Error>> {
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");
    let api_path = "/0/private/TradeVolume";
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
        .post("https://api.kraken.com/0/private/TradeVolume")
        .headers(headers)
        .body(api_post)
        .send()
        .await?;

    let response_text = res.text().await?;
    let v: Value = serde_json::from_str(&response_text)?;
    let volume = v["result"]["volume"]
        .as_str()
        .unwrap()
        .parse::<f64>()
        .unwrap();

    Ok(volume)
}

pub async fn execute_trade(
    path_names: Vec<String>,
    rates_expect: &Vec<f64>,
    min_volume: f64,
    assets_to_pair: &AssetsToPair,
    pair_to_spread: PairToSpread,
    private_ws: Arc<tokio::sync::Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    token: &str,
    fees: &HashMap<String, f64>,
    orders: &Arc<Mutex<OrderMap>>,
    client: Arc<Client>,
    graph_id: i64,
    recent_latency: f64,
    winnings_expected: f64,
    roi_expected: f64,
) -> () {
    let starting_volume = min_volume;
    let mut asset1_volume = starting_volume;
    let mut remaining_asset1_volume: f64; // For determing how much to "sell back to starter"
    let private_ws_clone = private_ws.clone();
    let private_ws_clone2 = private_ws.clone();

    let mut rates_act = Vec::<f64>::new();
    let start_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    for i in 0..path_names.len() - 1 {
        let asset1 = &path_names[i];
        let asset2 = &path_names[i + 1];
        let pair_data = match assets_to_pair.get(&(asset1.to_string(), asset2.to_string())) {
            Some(pair_data) => pair_data,
            None => {
                log::error!("Trade failed: No {} & {} pair", asset1, asset2);
                return;
            }
        };
        let pair = &pair_data.pair;
        let base = &pair_data.base;
        let (buy_sell, trade_volume) = match determine_trade_info(
            asset1,
            asset2,
            base,
            pair,
            asset1_volume,
            fees[pair],
            &pair_to_spread,
        ) {
            Ok((buy_sell, trade_volume)) => (buy_sell, trade_volume),
            Err(e) => {
                log::error!("Error determining trade info: {:?}", e);
                return;
            }
        };

        // Allow limit orders beyond expectations based on remaining ROI
        let remaining_roi = compute_roi(rates_expect, &rates_act);
        let price: f64;
        let price_expected: f64;
        if buy_sell == "buy" {
            price_expected = pair_to_spread[pair].ask;
            price = pair_to_spread[pair].ask * (1. + remaining_roi);
        } else {
            price_expected = pair_to_spread[pair].bid;
            price = pair_to_spread[pair].bid * (1. - remaining_roi);
        }

        let send_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let userref = match make_trade(
            token,
            &buy_sell,
            trade_volume,
            price,
            pair,
            Arc::clone(&private_ws_clone),
        ) {
            Ok(userref) => userref,
            Err(e) => {
                log::error!("Error making trade: {:?}", e);
                return;
            }
        };
        let mut order_data = None;
        let start = Instant::now();
        while start.elapsed().as_millis() < 50 {
            let order_exists = {
                let orders = orders.lock().unwrap();
                orders.contains_key(&userref)
            };
            if order_exists {
                let orders = orders.lock().unwrap();
                order_data = orders.get(&userref).cloned();
                break;
            }
            tokio::time::sleep(Duration::from_micros(5)).await;
        }
        let response_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // Log individual trade details
        let pair_clone = pair.clone();
        let order_data_clone = order_data.clone();
        let client_clone = Arc::clone(&client);
        let buy_sell_clone = buy_sell.clone();
        tokio::spawn(async move {
            trade_leg_to_influx(
                client_clone,
                order_data_clone,
                graph_id,
                pair_clone,
                (i + 1) as i64,
                recent_latency,
                send_ts,
                response_ts,
                buy_sell_clone,
                trade_volume,
                price_expected,
                false,
            )
            .await;
        });

        if let Some(order_data) = order_data {
            let vol = order_data.vol;
            let cost = order_data.cost;
            let fee = order_data.fee;
            if buy_sell == "buy" {
                rates_act.push(vol / (cost + fee));
                remaining_asset1_volume = asset1_volume - (cost + fee);
                asset1_volume = vol; // Update for next iteration
            } else {
                rates_act.push((cost - fee) / vol);
                remaining_asset1_volume = asset1_volume - vol;
                asset1_volume = cost - fee; // Update for next iteration
            }
        } else {
            // Order never received: Throw error
            let _ = trade_back_to_starter(
                &asset1,
                &path_names,
                &assets_to_pair,
                asset1_volume,
                &fees,
                &pair_to_spread,
                token,
                private_ws_clone,
            );
            log::warn!("Attempted trade yielded no order.");
            return;
        }
        // Trade back to the starter
        let _ = trade_back_to_starter(
            &asset1,
            &path_names,
            &assets_to_pair,
            remaining_asset1_volume,
            &fees,
            &pair_to_spread,
            token,
            private_ws_clone2.clone(),
        );
    }
    let ending_volume = asset1_volume;
    let end_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    let client_clone = Arc::clone(&client);
    let winnings_actual = ending_volume - starting_volume;
    let roi_actual = ending_volume / starting_volume - 1.;
    tokio::spawn(async move {
        trade_path_to_influx(
            client_clone,
            graph_id,
            path_names,
            recent_latency,
            start_ts,
            end_ts,
            winnings_expected,
            winnings_actual,
            roi_expected,
            roi_actual,
        )
        .await;
    });
}

/// Return the ROI allocation for the trade,
/// based on prior actual trade prices and future expected trade prices
fn compute_roi(rates_expect: &Vec<f64>, rates_act: &Vec<f64>) -> f64 {
    let total = rates_expect.len();
    let complete = rates_act.len();
    let mut roi = 1.;
    for rate in rates_act {
        roi *= rate;
    }
    for rate in rates_expect.iter().skip(complete) {
        roi *= rate;
    }
    roi -= 1.;
    1. / ((total as f64) - (complete as f64)) * roi
}

fn determine_trade_info(
    asset1: &String,
    asset2: &String,
    base: &String,
    pair: &String,
    volume: f64,
    fee_pct: f64,
    pair_to_spread: &PairToSpread,
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

fn make_trade(
    token: &str,
    trade_type: &String,
    volume: f64,
    price: f64,
    pair: &String,
    private_ws: Arc<tokio::sync::Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let userref = rand::random::<i32>();

    let trade_msg: String;
    if trade_type == "buy" {
        trade_msg = serde_json::json!({
            "event": "addOrder",
            "token": token,
            "type": "buy",
            "ordertype": "limit",
            "timeinforce": "IOC",
            "price": price,
            "volume": volume,
            "pair": pair,
            "userref": userref,
        })
        .to_string();
    } else {
        trade_msg = serde_json::json!({
            "event": "addOrder",
            "token": token,
            "type": "sell",
            "ordertype": "market",
            "volume": volume,
            "pair": pair,
            "userref": userref,
        })
        .to_string();
    }
    let private_ws_clone = Arc::clone(&private_ws);
    tokio::spawn(async move {
        let mut lock = private_ws_clone.lock().await;
        let _ = lock.send(Message::Text(trade_msg)).await; // Don't wait for trade to go through
    });
    Ok(userref)
}

fn trade_back_to_starter(
    asset1: &String,
    path_names: &Vec<String>,
    assets_to_pair: &AssetsToPair,
    remaining_asset1_volume: f64,
    fees: &HashMap<String, f64>,
    pair_to_spread: &PairToSpread,
    token: &str,
    private_ws: Arc<tokio::sync::Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let pair_data = assets_to_pair
        .get(&(asset1.to_string(), path_names[0].to_string()))
        .ok_or(format!(
            "Trade failed: No {} & {} pair",
            asset1, &path_names[0]
        ))?;
    let pair = &pair_data.pair;
    let base = &pair_data.base;
    let (buy_sell, trade_volume) = determine_trade_info(
        asset1,
        &path_names[0],
        base,
        pair,
        remaining_asset1_volume,
        fees[pair],
        pair_to_spread,
    )?;
    let _ = make_trade(
        token,
        &buy_sell,
        trade_volume,
        pair_to_spread[pair].ask,
        pair,
        private_ws,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_roi() {
        let rates_expect = vec![2.0, 0.5, 4.0, 0.26];
        let rates_act = vec![1.95, 0.55];

        let roi = compute_roi(&rates_expect, &rates_act);

        assert!((roi - 0.0577).abs() < 0.0001);
    }
}
