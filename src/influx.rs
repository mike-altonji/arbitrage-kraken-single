use influx_db_client::{reqwest::Url, Client, Point};
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

pub async fn spread_latency_from_influx(
    p90_latency: Arc<Mutex<f64>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (client, retention_policy, _, _) = setup_influx().await;
    let query = format!(
        "SELECT last(latency) FROM \"{}\".\"recent_latency\"",
        *retention_policy
    );
    let result = client.query(&query, None).await?;
    let res2 = result.ok_or_else(|| "No Vec<Node> found")?;
    let series = res2[0].series.as_ref().ok_or_else(|| "No series found")?;
    let values = series[0].values.as_ref().ok_or_else(|| "No values found")?;
    let value = values.get(0).ok_or_else(|| "No value found")?;
    let latency = value.get(1).ok_or_else(|| "No latency found")?;
    let latency_value = latency.as_f64().ok_or_else(|| "Latency is not a float")?;
    match p90_latency.lock() {
        Ok(mut p90_latency_lock) => *p90_latency_lock = latency_value,
        Err(e) => log::error!("Failed to acquire lock: {:?}", e),
    }

    Ok(())
}
