use crate::utils;
use dotenv::dotenv;
use std::env;
use std::sync::atomic::{AtomicI16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

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
) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");

    loop {
        if let Err(e) = update_fees(&api_key, &api_secret, fee_spot, fee_stablecoin).await {
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
) -> Result<(), Box<dyn std::error::Error>> {
    let api_path = "/0/private/TradeVolume";
    let (api_post, headers) =
        utils::get_api_params(api_key, api_secret, api_path, Some("pair=XBTUSD,USDTUSD"))?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.kraken.com/0/private/TradeVolume")
        .headers(headers)
        .body(api_post)
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;

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
    }

    // Extract stablecoin fee
    if let Some(fee_f64) = extract_fee_from_pair(fees, "USDTZUSD") {
        // Convert percentage to basis points (0.20% = 20)
        let fee_basis_points = (fee_f64 * 100.0).round() as i16;
        fee_stablecoin.store(fee_basis_points, Ordering::Relaxed);
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
