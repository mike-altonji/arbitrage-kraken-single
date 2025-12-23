use crate::utils;
use dotenv::dotenv;
use std::env;
use std::time::Duration;
use tokio::time::sleep;

/// Fetch asset balances from Kraken API
pub async fn fetch_asset_balances(
    asset_balances: &mut (i16, i16),
) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let api_key = env::var("KRAKEN_KEY").expect("KRAKEN_KEY must be set");
    let api_secret = env::var("KRAKEN_SECRET").expect("KRAKEN_SECRET must be set");

    loop {
        if let Err(e) = update_balances(&api_key, &api_secret, asset_balances).await {
            log::error!("Error fetching balances: {}", e);
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Update asset balances from Kraken API
/// asset_balances: (USD_balance, EUR_balance) as i16
async fn update_balances(
    api_key: &str,
    api_secret: &str,
    asset_balances: &mut (i16, i16),
) -> Result<(), Box<dyn std::error::Error>> {
    let api_path = "/0/private/Balance";
    let (post, headers) = utils::get_api_params(api_key, api_secret, api_path)?;

    let client = reqwest::Client::new();
    let resp = client
        .post("https://api.kraken.com/0/private/Balance")
        .headers(headers)
        .body(post)
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;

    // Extract result object
    let result = data
        .get("result")
        .and_then(|r| r.as_object())
        .ok_or("Missing or invalid result in Balance response")?;

    // Parse USD and EUR balances
    let mut usd_balance = 0i16;
    let mut eur_balance = 0i16;

    for (key, value) in result {
        let balance_str = value.as_str().ok_or("Balance value is not a string")?;
        let balance = balance_str.parse::<f64>()?.floor() as i16;
        match key.as_str() {
            "ZUSD" => usd_balance = balance,
            "ZEUR" => eur_balance = balance,
            _ => {} // Ignore other currencies
        }
    }

    // Update the tuple
    *asset_balances = (usd_balance, eur_balance);

    Ok(())
}
