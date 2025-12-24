use crate::structs::{PairData, PairDataVec};
use base64::{decode_config, encode_config, STANDARD};
use hmac::{Hmac, Mac, NewMac};
use log4rs::{append::file::FileAppender, config};
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task;

/// Initialize logging. Will create a file in `logs/arbitrage_log_{timestamp}.log`
/// If debug_mode is true, will log at Debug level. Otherwise, will log at Info level.
pub fn init_logging(debug_mode: bool) {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time invalid");
    let timestamp = since_the_epoch.as_secs();
    let log_config = FileAppender::builder()
        .build(format!("logs/arbitrage_log_{}.log", timestamp))
        .expect("Unable to build log file");
    let log_config = config::Config::builder()
        .appender(config::Appender::builder().build("default", Box::new(log_config)))
        .build(
            config::Root::builder()
                .appender("default")
                .build(if debug_mode {
                    log::LevelFilter::Debug
                } else {
                    log::LevelFilter::Info
                }),
        )
        .expect("Unable to build log file");
    log4rs::init_config(log_config).expect("Unable to build log file");
}

/// Send a Telegram message to the configured chat ID.
pub async fn send_telegram_message(message: &str) {
    let bot_token = env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN must be set");
    let chat_id = env::var("TELEGRAM_CHAT_ID").expect("TELEGRAM_CHAT_ID must be set");

    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}",
        bot_token, chat_id, message
    );

    if let Err(e) = reqwest::Client::new().post(&url).send().await {
        log::error!("Failed to send Telegram message: {:?}", e);
    }
}

/// Build a vector of pair names indexed by their position in the asset_index map
/// Returns a vector where pair_names[idx] gives the pair name for that index
pub fn build_pair_names_vec(asset_index: &phf::Map<&'static str, usize>) -> Vec<&'static str> {
    let max_idx = asset_index
        .values()
        .max()
        .copied()
        .expect("No pairs in asset_index");
    let mut names = vec![""; max_idx + 1];
    for (name, &idx) in asset_index.entries() {
        names[idx] = name;
    }
    names
}

/// Fetch asset pairs metadata from Kraken API and initialize PairDataVec
/// Returns a vector of PairData with default values for prices/volumes and real metadata from API
/// The vector is ordered by the indices in the asset_index map
pub async fn initialize_pair_data(asset_index: &phf::Map<&'static str, usize>) -> PairDataVec {
    // Fetch asset pairs from Kraken API
    let asset_pairs_url = "https://api.kraken.com/0/public/AssetPairs";
    let resp = reqwest::get(asset_pairs_url).await.expect("No pairs");
    let text = resp.text().await.expect("No pairs text");
    let data: serde_json::Value = serde_json::from_str(&text).expect("No pairs data");
    let pairs_obj = data["result"].as_object().expect("No pairs in response");

    // Create a map from wsname to pair data for quick lookup
    let mut wsname_to_data: std::collections::HashMap<String, (usize, usize, f64, f64, bool)> =
        std::collections::HashMap::new();

    for (key, details) in pairs_obj {
        let wsname = details["wsname"]
            .as_str()
            .expect(&format!("No wsname for {}", key))
            .to_string();
        let status = details["status"]
            .as_str()
            .expect(&format!("No status for {}", wsname))
            == "online";
        let price_decimals = details["pair_decimals"]
            .as_u64()
            .expect(&format!("No pair_decimals for {}", wsname))
            as usize;
        let volume_decimals = details["lot_decimals"]
            .as_u64()
            .expect(&format!("No lot_decimals for {}", wsname))
            as usize;
        let order_min = details["ordermin"]
            .as_str()
            .expect(&format!("No ordermin for {}", wsname))
            .parse::<f64>()
            .expect(&format!("Failed to parse ordermin for {}", wsname));
        let cost_min = details["costmin"]
            .as_str()
            .expect(&format!("No costmin for {}", wsname))
            .parse::<f64>()
            .expect(&format!("Failed to parse costmin for {}", wsname));

        wsname_to_data.insert(
            wsname,
            (price_decimals, volume_decimals, order_min, cost_min, status),
        );
    }

    // Collect pairs with their indices and sort by index
    let mut pairs_with_indices: Vec<(usize, String)> = asset_index
        .entries()
        .map(|(pair, &idx)| (idx, pair.to_string()))
        .collect();
    pairs_with_indices.sort_by_key(|(idx, _)| *idx);

    // Initialize PairDataVec with default values and metadata from API, ordered by index
    let mut pair_data_vec = Vec::with_capacity(pairs_with_indices.len());
    for (_, pair) in pairs_with_indices {
        let (price_decimals, volume_decimals, order_min, cost_min, pair_status) = wsname_to_data
            .get(&pair)
            .copied()
            .expect(&format!("No data for {}", pair));

        // Push default values which will be updated by ws events
        pair_data_vec.push(PairData {
            bid_price: 0.0,
            ask_price: 0.0,
            bid_volume: 100.0,
            ask_volume: 100.0,
            price_decimals,
            volume_decimals,
            order_min,
            cost_min,
            pair_status,
        });
    }

    pair_data_vec
}

/// Helper function to get API post, signature and headers for private API calls
pub fn get_api_params(
    api_key: &str,
    api_secret: &str,
    api_path: &str,
    api_post_body: Option<&str>,
) -> Result<(String, HeaderMap), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();

    // Build api_post with optional body appended
    let api_post = match api_post_body {
        Some(body) if !body.is_empty() => format!("nonce={}&{}", nonce, body),
        _ => format!("nonce={}", nonce),
    };

    let nonce_bytes = nonce.as_bytes();
    let api_post_bytes = api_post.as_bytes();

    let mut hasher = Sha256::new();
    hasher.update(nonce_bytes);
    hasher.update(api_post_bytes);
    let api_sha256 = hasher.finalize();
    let api_secret_decoded = decode_config(&api_secret, STANDARD)
        .map_err(|e| format!("Invalid base64 in API secret: {}", e))?;
    let mut mac = Hmac::<Sha512>::new_varkey(&api_secret_decoded)
        .map_err(|_| "Invalid HMAC key: secret is empty or wrong length")?;
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

    Ok((api_post, headers))
}

/// Get Kraken authentication token for private API
pub async fn get_ws_auth_token() -> Result<String, Box<dyn std::error::Error>> {
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");
    let api_path = "/0/private/GetWebSocketsToken";
    let (post, headers) = get_api_params(&api_key, &api_secret, api_path, None)?;

    let client = reqwest::Client::new();
    let res = client
        .post("https://api.kraken.com/0/private/GetWebSocketsToken")
        .headers(headers)
        .body(post)
        .send()
        .await?;

    let response_text = res.text().await?;
    let v: Value = serde_json::from_str(&response_text)?;
    let token = v["result"]["token"].as_str().unwrap().to_string();

    Ok(token.to_string())
}

/// Wait for approximately N milliseconds using a high-resolution timer.
/// This is more accurate than OS sleep for sub-millisecond timing.
/// Yields periodically to avoid blocking the async runtime.
pub async fn wait_approx_ms(milliseconds: u64) {
    // First yield to ensure previous operations are processed
    task::yield_now().await;

    let target_duration = Duration::from_nanos(milliseconds * 1_000_000);
    let start = Instant::now();

    // Busy-wait with periodic yields for accuracy
    // Yield every ~100us to avoid blocking the executor
    let yield_interval = Duration::from_nanos(100_000);
    let mut last_yield = start;

    while start.elapsed() < target_duration {
        if last_yield.elapsed() >= yield_interval {
            task::yield_now().await;
            last_yield = Instant::now();
        }
        // Small spin loop to check time without yielding too frequently
        std::hint::spin_loop();
    }
}
