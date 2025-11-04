use crate::structs::OrderData;
use influx_db_client::{Client, Point, Precision, Value};
use std::{collections::HashSet, sync::Arc};

/// Log a trade leg to Influx
pub async fn trade_leg_to_influx(
    client: Arc<Client>,
    userref: i32,
    path_uuid: String,
    order: Option<OrderData>,
    graph_id: i64,
    pair: String,
    trade_number: i64,
    send_ts: f64,
    response_ts: f64,
    trade_direction: String,
    volume_expected: f64,
    price_expected: f64,   // The expected bid or ask price from spread. No fees.
    back_to_starter: bool, // bool for if this is us reverting to the starting currency
) {
    let trade_direction_clone = trade_direction.clone();
    let mut point = Point::new("trade_leg")
        .add_field("userref", Value::Integer(userref as i64))
        .add_field("path_uuid", Value::String(path_uuid))
        .add_field("graph_id", Value::Integer(graph_id))
        .add_field("pair", Value::String(pair))
        .add_field("trade_number", Value::Integer(trade_number))
        .add_field("send_ts", Value::Float(send_ts))
        .add_field("trade_direction", Value::String(trade_direction))
        .add_field("volume_expected", Value::Float(volume_expected))
        .add_field("price_expected", Value::Float(price_expected))
        .add_field("back_to_starter", Value::Boolean(back_to_starter));

    if let Some(order) = order {
        let send_to_execute = order.lastupdated - send_ts;
        let send_to_response = response_ts - send_ts;
        let price_pct_diff = order.price / price_expected - 1.;
        let got_good_price = match trade_direction_clone.as_str() {
            "sell" if price_pct_diff >= 0. => 1.,
            "buy" if price_pct_diff <= 0. => 1.,
            _ => 0.,
        };
        point = point
            .add_field("send_to_execute", Value::Float(send_to_execute))
            .add_field("send_to_response", Value::Float(send_to_response))
            .add_field("order_id", Value::String(order.order_id))
            .add_field("volume_actual", Value::Float(order.vol))
            .add_field("price_actual", Value::Float(order.price))
            .add_field("cost_actual", Value::Float(order.cost))
            .add_field("fee_actual", Value::Float(order.fee))
            .add_field("price_pct_diff", Value::Float(price_pct_diff))
            .add_field("win", Value::Float(got_good_price));
    }

    let _ = client
        .write_point(point, Some(Precision::Nanoseconds), None)
        .await
        .expect("Failed to write to trade_leg");
}

/// Log overall trade results Influx
pub async fn trade_path_to_influx(
    client: Arc<Client>,
    path_uuid: String,
    graph_id: i64,
    path: Vec<String>,
    start_ts: f64,
    end_ts: f64,
    winnings_expected: f64,
    winnings_actual: f64,
    roi_expected: f64,
    roi_actual: f64,
) {
    let point = Point::new("trade_path")
        .add_field("path_uuid", Value::String(path_uuid))
        .add_field("graph_id", Value::Integer(graph_id))
        .add_field("path", Value::String(path.join("|")))
        .add_field("start_ts", Value::Float(start_ts))
        .add_field("duration", Value::Float(end_ts - start_ts))
        .add_field("winnings_expected", Value::Float(winnings_expected))
        .add_field("winnings_actual", Value::Float(winnings_actual))
        .add_field("roi_expected", Value::Float(roi_expected))
        .add_field("roi_actual", Value::Float(roi_actual))
        .add_field("roi_pct_diff", Value::Float(roi_actual / roi_expected - 1.))
        .add_field(
            "win",
            Value::Float(if winnings_actual > 0. { 1. } else { 0. }),
        );

    let _ = client
        .write_point(point, Some(Precision::Nanoseconds), None)
        .await
        .expect("Failed to write to trade_path");
}

/// Rotate the cycle-path such that it starts at the first `starter` found.
/// If no starters, do nothing.
pub fn rotate_path(path: &mut Vec<String>, starters: &HashSet<String>) {
    path.pop(); // Pop the final asset, breaking the cycle
    if let Some(index) = path.iter().position(|asset| starters.contains(asset)) {
        path.rotate_left(index);
    }

    // Add the first asset back to the end to complete the cycle
    if let Some(first_asset) = path.first() {
        path.push(first_asset.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_rotate_path() {
        let starters = HashSet::from(["USD".to_string(), "EUR".to_string()]);

        let mut path1 = vec![
            "BTC".to_string(),
            "EUR".to_string(),
            "DOGE".to_string(),
            "USD".to_string(),
            "BTC".to_string(),
        ];

        rotate_path(&mut path1, &starters);

        let mut path2 = vec!["BTC".to_string(), "DOGE".to_string(), "BTC".to_string()];

        rotate_path(&mut path2, &starters);

        assert_eq!(
            path1,
            vec![
                "EUR".to_string(),
                "DOGE".to_string(),
                "USD".to_string(),
                "BTC".to_string(),
                "EUR".to_string(),
            ]
        );

        assert_eq!(
            path2,
            vec!["BTC".to_string(), "DOGE".to_string(), "BTC".to_string(),]
        );
    }
}
