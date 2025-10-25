use influx_db_client::{reqwest::Url, Client, Point};
use std::error::Error;
use std::sync::{Arc, Mutex};

pub async fn setup_influx() -> (Arc<Client>, Arc<String>, usize, Vec<Point>) {
    dotenv::dotenv().ok();
    let host = std::env::var("INFLUXDB_HOST").expect("INFLUXDB_HOST must be set");
    let port = std::env::var("INFLUXDB_PORT").expect("INFLUXDB_PORT must be set");
    let db_name = std::env::var("DB_NAME").expect("DB_NAME must be set");
    let user = std::env::var("DB_USER").expect("DB_USER must be set");
    let password = std::env::var("DB_PASSWORD").expect("DB_PASSWORD must be set");
    let retention_policy = Arc::new(std::env::var("RP_NAME").expect("RP_NAME must be set"));
    let batch_size: usize = 500;
    let points = Vec::new();
    let client = Arc::new(
        Client::new(
            Url::parse(&format!("http://{}:{}", &host, &port)).expect("InfluxDB URL unparseable"),
            &db_name,
        )
        .set_authentication(&user, &password),
    );

    (client, retention_policy, batch_size, points)
}

#[allow(dead_code)]
pub async fn spread_latency_from_influx(
    p90_latency: Arc<Mutex<f64>>,
) -> Result<(), Box<dyn Error>> {
    let (client, retention_policy, _, _) = setup_influx().await;
    let query = format!(
        "SELECT last(latency) FROM \"{}\".\"recent_latency\" WHERE time > now() - 1m",
        *retention_policy
    );
    let result = client.query(&query, None).await;
    let latency_value = match result {
        Ok(Some(data)) => data
            .get(0)
            .and_then(|first_result| first_result.series.as_ref())
            .and_then(|series| series.get(0))
            .and_then(|first_series| first_series.values.as_ref())
            .and_then(|values| values.get(0))
            .and_then(|first_value| first_value.get(1))
            .and_then(|latency| latency.as_f64())
            .unwrap_or(f64::MAX),
        _ => f64::MAX,
    };

    match p90_latency.lock() {
        Ok(mut p90_latency_lock) => *p90_latency_lock = latency_value,
        Err(e) => log::error!("Failed to acquire p90 latency lock: {:?}", e),
    }

    if latency_value.is_infinite() {
        log::warn!("Couldn't find a recent latency value: Setting to inf.");
    }

    Ok(())
}
