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
