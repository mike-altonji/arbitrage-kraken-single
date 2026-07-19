use crate::utils;
use crate::{asset_pairs, maker};
use dotenv::dotenv;
use std::env;
use std::sync::atomic::{AtomicI16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

/// One-shot startup pass (maker mode): seed maker positions from existing
/// account balances so coins held from previous runs immediately get resting
/// asks instead of sitting untracked forever. Basis is marked at the current
/// market mid (entry price from before the restart is unknown), so realized
/// PnL on these coins measures post-restart edge.
pub async fn seed_maker_inventory_from_balances() {
    dotenv().ok();
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");

    let balances = match fetch_balance_map(&api_key, &api_secret).await {
        Ok(b) => b,
        Err(e) => {
            log::error!("Inventory seeding: Balance fetch failed: {}", e);
            return;
        }
    };

    let mut seeded = 0usize;
    let mut total_value = 0.0f64;
    for (asset, qty) in balances {
        // Skip fiat, the FX stablecoin, and earn/staking sub-balances
        // (e.g. "SOL.F"); the maker can't quote those.
        if qty <= 0.0
            || asset.contains('.')
            || matches!(asset.as_str(), "ZUSD" | "ZEUR" | "USDT" | "USDC")
        {
            continue;
        }
        let Some(pair) = find_quotable_pair(&asset) else {
            log::debug!("Inventory seeding: no quotable pair for {} ({})", asset, qty);
            continue;
        };
        let mid = match fetch_public_mid(pair).await {
            Ok(m) => m,
            Err(e) => {
                log::warn!("Inventory seeding: no mid for {}: {}", pair, e);
                continue;
            }
        };
        let value = qty * mid;
        if value < 0.50 {
            log::debug!("Inventory seeding: {} is dust (${:.2}), skipping", pair, value);
            continue;
        }
        if maker::seed_position(pair, qty, value) {
            seeded += 1;
            total_value += value;
            log::info!(
                "Seeded {}: {} coins (~${:.2} @ mid {}) — ask will rest until sold",
                pair,
                qty,
                value,
                mid
            );
        }
        // Public endpoint, but be polite at startup.
        sleep(Duration::from_millis(100)).await;
    }
    if seeded > 0 {
        log::info!(
            "Inventory seeding complete: {} pairs, ~${:.2} total basis (counts against --maker-global-notional)",
            seeded,
            total_value
        );
    }
}

/// Map a Kraken Balance asset code to a quotable pair, preferring USD.
/// Legacy assets carry an X prefix in Balance (e.g. XXBT → XBT/USD).
fn find_quotable_pair(asset: &str) -> Option<&'static str> {
    let mut candidates = vec![asset.to_string()];
    if asset.len() == 4 && (asset.starts_with('X') || asset.starts_with('Z')) {
        candidates.push(asset[1..].to_string());
    }
    for base in &candidates {
        for quote in ["USD", "EUR"] {
            if let Some(pair) = asset_pairs::find_pair(&format!("{base}/{quote}")) {
                return Some(pair);
            }
        }
    }
    None
}

/// All balances as (asset, quantity) pairs.
async fn fetch_balance_map(
    api_key: &str,
    api_secret: &str,
) -> Result<Vec<(String, f64)>, Box<dyn std::error::Error>> {
    let api_path = "/0/private/Balance";
    let (post, headers) = utils::get_api_params(api_key, api_secret, api_path, None)?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.kraken.com/0/private/Balance")
        .headers(headers)
        .body(post)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("Kraken API returned status: {}", resp.status()).into());
    }
    let data: serde_json::Value = resp.json().await?;
    if let Some(errors) = data.get("error").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            let error_msg = errors
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!("Kraken API error: {}", error_msg).into());
        }
    }
    let result = data
        .get("result")
        .and_then(|r| r.as_object())
        .ok_or("Missing or invalid result in Balance response")?;

    Ok(result
        .iter()
        .filter_map(|(k, v)| {
            v.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|q| (k.clone(), q))
        })
        .collect())
}

/// Current mid price for a pair from the public Ticker endpoint. Requested
/// one pair at a time so the (canonically renamed) result key doesn't matter.
async fn fetch_public_mid(pair: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let altname = pair.replace('/', "");
    let url = format!("https://api.kraken.com/0/public/Ticker?pair={altname}");
    let data: serde_json::Value = reqwest::get(&url).await?.json().await?;
    if let Some(errors) = data.get("error").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            let error_msg = errors
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!("Kraken API error: {}", error_msg).into());
        }
    }
    let ticker = data
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|o| o.values().next())
        .ok_or("Empty Ticker result")?;
    let leg = |key: &str| -> Option<f64> {
        ticker
            .get(key)?
            .as_array()?
            .first()?
            .as_str()?
            .parse::<f64>()
            .ok()
    };
    let (bid, ask) = (leg("b").ok_or("no bid")?, leg("a").ok_or("no ask")?);
    if bid <= 0.0 || ask <= 0.0 {
        return Err("non-positive bid/ask".into());
    }
    Ok((bid + ask) / 2.0)
}

/// Fetch asset balances from Kraken API every 2 seconds
/// balance: atomic i16 representing a fiat balance
pub async fn fetch_asset_balances(usd_balance: &AtomicI16, eur_balance: &AtomicI16) {
    dotenv().ok();
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");

    loop {
        if let Err(e) = update_balances(&api_key, &api_secret, usd_balance, eur_balance).await {
            log::error!("Error fetching balances: {}", e);
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Update asset balances from Kraken API
/// balance: atomic i16 representing a fiat balance
async fn update_balances(
    api_key: &str,
    api_secret: &str,
    usd_balance: &AtomicI16,
    eur_balance: &AtomicI16,
) -> Result<(), Box<dyn std::error::Error>> {
    let api_path = "/0/private/Balance";
    let (post, headers) = utils::get_api_params(api_key, api_secret, api_path, None)?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.kraken.com/0/private/Balance")
        .headers(headers)
        .body(post)
        .send()
        .await?;

    // Check HTTP status code
    if !resp.status().is_success() {
        return Err(format!("Kraken API returned status: {}", resp.status()).into());
    }

    let data: serde_json::Value = resp.json().await?;

    // Check for Kraken API errors
    if let Some(errors) = data.get("error").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            let error_msg = errors
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!("Kraken API error: {}", error_msg).into());
        }
    }

    // Extract result object
    let result = data
        .get("result")
        .and_then(|r| r.as_object())
        .ok_or("Missing or invalid result in Balance response")?;

    // Track if we found the balances we care about
    let mut found_usd = false;
    let mut found_eur = false;

    // Parse USD and EUR balances
    for (key, value) in result {
        let balance_str = value.as_str().ok_or("Balance value is not a string")?;
        let balance = balance_str.parse::<f64>()?.floor() as i16;
        match key.as_str() {
            "ZUSD" => {
                usd_balance.store(balance, Ordering::Relaxed);
                found_usd = true;
                log::debug!("USD balance: {}", balance);
            }
            "ZEUR" => {
                eur_balance.store(balance, Ordering::Relaxed);
                found_eur = true;
                log::debug!("EUR balance: {}", balance);
            }
            _ => {} // Ignore other currencies
        }
    }

    // Warn if expected balances are missing
    if !found_usd {
        log::warn!("USD balance (ZUSD) not found in API response");
    }
    if !found_eur {
        log::warn!("EUR balance (ZEUR) not found in API response");
    }

    Ok(())
}

/// Fetch trading fees from Kraken API every 5 minutes
/// fee: atomic i16 representing basis points (e.g., 40 = 0.40%)
pub async fn fetch_trading_fees(
    fee_spot: &AtomicI16,
    fee_stablecoin: &AtomicI16,
    fee_maker: &AtomicI16,
) {
    dotenv().ok();
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");

    loop {
        if let Err(e) =
            update_fees(&api_key, &api_secret, fee_spot, fee_stablecoin, fee_maker).await
        {
            log::error!("Error fetching fees: {}", e);
        }
        sleep(Duration::from_secs(300)).await; // 5 minutes
    }
}

/// Update trading fees from Kraken API
/// Uses TradingVolume endpoint with representative pairs to get fee tiers
async fn update_fees(
    api_key: &str,
    api_secret: &str,
    fee_spot: &AtomicI16,
    fee_stablecoin: &AtomicI16,
    fee_maker: &AtomicI16,
) -> Result<(), Box<dyn std::error::Error>> {
    let api_path = "/0/private/TradeVolume";
    let (post, headers) =
        utils::get_api_params(api_key, api_secret, api_path, Some("pair=XBTUSD,USDTUSD"))?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.kraken.com/0/private/TradeVolume")
        .headers(headers)
        .body(post)
        .send()
        .await?;

    // Check HTTP status code
    if !resp.status().is_success() {
        return Err(format!("Kraken API returned status: {}", resp.status()).into());
    }

    let data: serde_json::Value = resp.json().await?;

    // Check for Kraken API errors
    if let Some(errors) = data.get("error").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            let error_msg = errors
                .iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!("Kraken API error: {}", error_msg).into());
        }
    }

    // Extract fees from result
    let result = data
        .get("result")
        .and_then(|r| r.as_object())
        .ok_or("Missing or invalid result in TradeVolume response")?;

    // Get fees object
    let fees = result
        .get("fees")
        .and_then(|f| f.as_object())
        .ok_or("Missing fees in TradeVolume response")?;

    // Extract spot fee
    if let Some(fee_f64) = extract_fee_from_pair(fees, "XXBTZUSD") {
        // Convert percentage to basis points (0.40% = 40)
        let fee_basis_points = (fee_f64 * 100.0).round() as i16;
        fee_spot.store(fee_basis_points, Ordering::Relaxed);
        log::debug!("Spot fee: {} bps", fee_basis_points);
    } else {
        log::warn!("Spot fee not found in API response");
    }

    // Extract stablecoin fee
    if let Some(fee_f64) = extract_fee_from_pair(fees, "USDTZUSD") {
        // Convert percentage to basis points (0.20% = 20)
        let fee_basis_points = (fee_f64 * 100.0).round() as i16;
        fee_stablecoin.store(fee_basis_points, Ordering::Relaxed);
        log::debug!("Stablecoin fee: {} bps", fee_basis_points);
    } else {
        log::warn!("Stablecoin fee not found in API response");
    }

    // Extract maker fee (separate schedule from the taker "fees" object)
    if let Some(fees_maker) = result.get("fees_maker").and_then(|f| f.as_object()) {
        if let Some(fee_f64) = extract_fee_from_pair(fees_maker, "XXBTZUSD") {
            let fee_basis_points = (fee_f64 * 100.0).round() as i16;
            fee_maker.store(fee_basis_points, Ordering::Relaxed);
            log::debug!("Maker fee: {} bps", fee_basis_points);
        } else {
            log::warn!("Maker fee not found in fees_maker response");
        }
    } else {
        log::warn!("fees_maker not found in TradeVolume response; keeping conservative default");
    }

    Ok(())
}

/// Extract fee value from a pair in the fees object
/// Returns None if any step fails
fn extract_fee_from_pair(
    fees: &serde_json::Map<String, serde_json::Value>,
    pair: &str,
) -> Option<f64> {
    let fee_obj = fees.get(pair)?.as_object()?;
    let fee_value = fee_obj.get("fee")?;
    let fee_str = fee_value.as_str()?;
    fee_str.parse::<f64>().ok()
}
