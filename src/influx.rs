use influx_db_client::{reqwest::Url, Client, Point, Precision, Value};

pub async fn setup_influx() -> (Client, String, usize, Vec<Point>) {
    dotenv::dotenv().ok();
    let host = std::env::var("INFLUXDB_HOST").expect("INFLUXDB_HOST must be set");
    let port = std::env::var("INFLUXDB_PORT").expect("INFLUXDB_PORT must be set");
    let db_name = std::env::var("DB_NAME").expect("DB_NAME must be set");
    let user = std::env::var("DB_USER").expect("DB_USER must be set");
    let password = std::env::var("DB_PASSWORD").expect("DB_PASSWORD must be set");
    let retention_policy = std::env::var("RP_NAME").expect("RP_NAME must be set");
    let batch_size: usize = 500;
    let points = Vec::new();
    let client = Client::new(
        Url::parse(&format!("http://{}:{}", &host, &port)).expect("InfluxDB URL unparseable"),
        &db_name,
    )
    .set_authentication(&user, &password);

    (client, retention_policy, batch_size, points)
}

/// Log kraken timestamp to ingestion timestamp latency
pub fn log_kraken_ingestion_latency(pair: &str, kraken_ts: f64, ingestion_ts: f64) {
    let latency = ingestion_ts - kraken_ts;
    let pair = pair.to_string();
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point = Point::new("kraken_ingestion_latency")
            .add_tag("pair", Value::String(pair))
            .add_field("kraken_ts", Value::Float(kraken_ts))
            .add_field("ingestion_ts", Value::Float(ingestion_ts))
            .add_field("latency", Value::Float(latency));
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}

/// Log arbitrage evaluation speed
pub fn log_arbitrage_evaluation_speed(start_ts: u128, end_ts: u128) {
    let duration = (end_ts - start_ts) as f64 / 1_000_000_000.0;
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point =
            Point::new("arbitrage_evaluation_speed").add_field("duration", Value::Float(duration));
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}

/// Log overall listener loop speed
pub fn log_listener_loop_speed(loop_start: u128, loop_end: u128) {
    let duration = (loop_end - loop_start) as f64 / 1_000_000_000.0;
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point = Point::new("listener_loop_speed").add_field("duration", Value::Float(duration));
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}

/// Log trade thread message receive speed (time from send to receive)
pub fn log_trade_message_receive_speed(send_timestamp: u128, receive_timestamp: u128) {
    let duration = (receive_timestamp - send_timestamp) as f64 / 1_000_000_000.0;
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point =
            Point::new("trade_message_receive_speed").add_field("duration", Value::Float(duration));
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}

/// Log time between trades (1-2, 2-3, 3-4) to ensure our wait times are correct
/// Currently only used for 1-2, which is the only time-sensitive interval
pub fn log_trade_interval(interval_name: &str, duration_ms: f64) {
    let interval_name = interval_name.to_string();
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point = Point::new("trade_interval")
            .add_tag("interval_name", Value::String(interval_name))
            .add_field("duration_ms", Value::Float(duration_ms));
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}

/// Log arbitrage opportunity details
pub fn log_arbitrage_opportunity(
    pair1_name: &str,
    pair2_name: &str,
    pair1_bid: f64,
    pair1_ask: f64,
    pair2_bid: f64,
    pair2_ask: f64,
    stable1_bid: f64,
    stable1_ask: f64,
    stable2_bid: f64,
    stable2_ask: f64,
    roi: f64,
    limiting_volume: f64,
    pair1_amount_in: f64,
    volume_limited_by_balance: bool,
) {
    let pair1_name = pair1_name.to_string();
    let pair2_name = pair2_name.to_string();
    tokio::spawn(async move {
        let (client, retention_policy, _, _) = setup_influx().await;
        let point = Point::new("arbitrage_opportunity")
            .add_tag("pair1", Value::String(pair1_name))
            .add_tag("pair2", Value::String(pair2_name))
            .add_field("pair1_bid", Value::Float(pair1_bid))
            .add_field("pair1_ask", Value::Float(pair1_ask))
            .add_field("pair2_bid", Value::Float(pair2_bid))
            .add_field("pair2_ask", Value::Float(pair2_ask))
            .add_field("stable1_bid", Value::Float(stable1_bid))
            .add_field("stable1_ask", Value::Float(stable1_ask))
            .add_field("stable2_bid", Value::Float(stable2_bid))
            .add_field("stable2_ask", Value::Float(stable2_ask))
            .add_field("roi", Value::Float(roi))
            .add_field("limiting_volume", Value::Float(limiting_volume))
            .add_field("pair1_amount_in", Value::Float(pair1_amount_in))
            .add_field(
                "volume_limited_by_balance",
                Value::Boolean(volume_limited_by_balance),
            );
        let _ = client
            .write_points(
                vec![point],
                Some(Precision::Nanoseconds),
                Some(&retention_policy),
            )
            .await;
    });
}
