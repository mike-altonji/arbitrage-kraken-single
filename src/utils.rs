use crate::structs::{PairData, PairDataVec};
use log4rs::{append::file::FileAppender, config};
use std::time::{SystemTime, UNIX_EPOCH};

/// Initialize logging. Will create a file in `logs/arbitrage_log_{timestamp}.log`
pub fn init_logging() {
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
                .build(log::LevelFilter::Info),
        )
        .expect("Unable to build log file");
    log4rs::init_config(log_config).expect("Unable to build log file");
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
    let mut wsname_to_data: std::collections::HashMap<String, (u8, u8, f32, f32, bool)> =
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
            .expect(&format!("No pair_decimals for {}", wsname)) as u8;
        let volume_decimals = details["lot_decimals"]
            .as_u64()
            .expect(&format!("No lot_decimals for {}", wsname)) as u8;
        let order_min = details["ordermin"]
            .as_str()
            .expect(&format!("No ordermin for {}", wsname))
            .parse::<f32>()
            .expect(&format!("Failed to parse ordermin for {}", wsname));
        let cost_min = details["costmin"]
            .as_str()
            .expect(&format!("No costmin for {}", wsname))
            .parse::<f32>()
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
