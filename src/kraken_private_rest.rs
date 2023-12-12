use base64::{decode_config, encode_config, STANDARD};
use dotenv::dotenv;
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256, Sha512};
use std::time::Duration;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;

use crate::structs::AssetNameConverter;

pub async fn fetch_asset_balances(
    asset_balances: &Arc<Mutex<HashMap<String, f64>>>,
    asset_name_conversion: &AssetNameConverter,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match update_balances(asset_balances, asset_name_conversion).await {
            Ok(_) => (),
            Err(e) => log::error!("Error fetching balances: {}", e),
        }
        println!("Balances: {:?}", asset_balances);
        sleep(Duration::from_secs(2)).await;
    }
}

pub async fn update_balances(
    asset_balances: &Arc<Mutex<HashMap<String, f64>>>,
    asset_name_conversion: &AssetNameConverter,
) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");
    let api_path = "/0/private/Balance";
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let api_post = format!("nonce={}", nonce);

    let nonce_bytes = nonce.as_bytes();
    let api_post_bytes = api_post.as_bytes();

    let mut hasher = Sha256::new();
    hasher.update(nonce_bytes);
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
    let resp = client
        .post("https://api.kraken.com/0/private/Balance")
        .headers(headers)
        .body(api_post)
        .send()
        .await
        .expect("Balance message send failed");

    let data: Result<serde_json::Value, _> = resp.json().await;
    match &data {
        Ok(_) => (),
        Err(e) => {
            log::error!("Failed to parse Balance response: {}", e);
        }
    };

    let data_balances: HashMap<String, f64> = match data {
        Ok(data) => {
            let mut balances: HashMap<String, f64> = HashMap::new();
            if let Some(result) = data.get("result") {
                if let Some(result_map) = result.as_object() {
                    for (key, value) in result_map {
                        if let Some(balance) = value.as_str() {
                            if let Ok(balance) = balance.parse::<f64>() {
                                if let Some(asset_ws) = asset_name_conversion.rest_to_ws(key) {
                                    balances.insert(asset_ws.clone(), balance);
                                }
                            }
                        }
                    }
                }
            }
            balances
        }
        Err(_) => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to parse Balance response",
            )))
        }
    };
    let mut asset_balances = asset_balances.lock().unwrap();
    *asset_balances = data_balances;

    Ok(())
}
