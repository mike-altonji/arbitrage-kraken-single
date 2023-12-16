#!/bin/bash

# Be sure to run `chmod +x scripts/setup_influxdb.sh` before running this for the first time

# Navigate to the parent directory where the .env file is located
cd "$(dirname "$0")/.."

# Source the environment variables from the .env file
if [ -f .env ]; then
  export $(egrep -v '^#' .env | xargs)
fi

# Create the InfluxDB database
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE DATABASE \"$DB_NAME\""

# Create the admin user
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE USER \"$DB_ADMIN_USER\" WITH PASSWORD '$DB_ADMIN_PASSWORD' WITH ALL PRIVILEGES"

# Create the regular user, and grant read/write permissions to the regular user on the specific database
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE USER \"$DB_USER\" WITH PASSWORD '$DB_PASSWORD'"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=GRANT READ ON \"$DB_NAME\" TO \"$DB_USER\""
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=GRANT WRITE ON \"$DB_NAME\" TO \"$DB_USER\""

# Create the retention policy
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE RETENTION POLICY \"$RP_NAME\" ON \"$DB_NAME\" DURATION $RP_DURATION REPLICATION 1"

# Create the continuous queries
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_spread_latency_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"latency\") AS count_values, mean(\"latency\") AS mean_latency, min(\"latency\") AS min_latency, max(\"latency\") AS max_latency, percentile(\"latency\", 1) AS p01_latency, percentile(\"latency\", 10) AS p10_latency, percentile(\"latency\", 25) AS p25_latency, percentile(\"latency\", 50) AS p50_latency, percentile(\"latency\", 75) AS p75_latency, percentile(\"latency\", 90) AS p90_latency, percentile(\"latency\", 99) AS p99_latency INTO \"spread_latency_aggregates\" FROM \"$RP_NAME\".\"spread_latency\" GROUP BY time(5m), pair END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_evaluation_time_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"duration\") AS count_values, mean(\"duration\") AS mean_duration, min(\"duration\") AS min_duration, max(\"duration\") AS max_duration, percentile(\"duration\", 1) AS p01_duration, percentile(\"duration\", 10) AS p10_duration, percentile(\"duration\", 25) AS p25_duration, percentile(\"duration\", 50) AS p50_duration, percentile(\"duration\", 75) AS p75_duration, percentile(\"duration\", 90) AS p90_duration, percentile(\"duration\", 99) AS p99_duration INTO \"evaluation_time_aggregates\" FROM \"$RP_NAME\".\"evaluation_time\" GROUP BY time(5m), graph_id END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_recent_latency\" ON \"$DB_NAME\" BEGIN SELECT percentile(\"latency\", 90) AS latency INTO \"$RP_NAME\".\"recent_latency\" FROM \"$RP_NAME\".\"spread_latency\" GROUP BY time(15s) END"

# Trade leg continuous queries
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_order_execution_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"send_to_execute\") AS count_values, mean(\"send_to_execute\") AS mean_latency, min(\"send_to_execute\") AS min_latency, max(\"send_to_execute\") AS max_latency, percentile(\"send_to_execute\", 25) AS p25_latency, percentile(\"send_to_execute\", 50) AS p50_latency, percentile(\"send_to_execute\", 75) AS p75_latency, percentile(\"send_to_execute\", 90) AS p90_latency INTO \"order_execution_aggregates\" FROM \"trade_leg\" GROUP BY time(1h) END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_order_roundtrip_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"send_to_response\") AS count_values, mean(\"send_to_response\") AS mean_latency, min(\"send_to_response\") AS min_latency, max(\"send_to_response\") AS max_latency, percentile(\"send_to_response\", 25) AS p25_latency, percentile(\"send_to_response\", 50) AS p50_latency, percentile(\"send_to_response\", 75) AS p75_latency, percentile(\"send_to_response\", 90) AS p90_latency INTO \"order_roundtrip_aggregates\" FROM \"trade_leg\" GROUP BY time(1h) END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_order_pricediff_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"price_pct_diff\") AS count_values, mean(\"price_pct_diff\") AS mean_pricediff, min(\"price_pct_diff\") AS min_pricediff, max(\"price_pct_diff\") AS max_pricediff, percentile(\"price_pct_diff\", 25) AS p25_pricediff, percentile(\"price_pct_diff\", 50) AS p50_pricediff, percentile(\"price_pct_diff\", 75) AS p75_pricediff INTO \"order_pricediff_aggregates\" FROM \"trade_leg\" GROUP BY time(1h), trade_number, trade_direction END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_order_wins_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"win\") AS total_orders, mean(\"win\") AS win_pct INTO \"order_wins_aggregates\" FROM \"trade_leg\" GROUP BY time(1h), trade_number, trade_direction END"

# Trade path continuous queries
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_path_roundtrip_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"duration\") AS count_values, mean(\"duration\") AS mean_latency, min(\"duration\") AS min_latency, max(\"duration\") AS max_latency, percentile(\"duration\", 25) AS p25_latency, percentile(\"duration\", 50) AS p50_latency, percentile(\"duration\", 75) AS p75_latency, percentile(\"duration\", 90) AS p90_latency INTO \"path_roundtrip_aggregates\" FROM \"trade_path\" GROUP BY time(1h) END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_path_wins_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"win\") AS total_orders, mean(\"win\") AS win_pct INTO \"path_wins_aggregates\" FROM \"trade_path\" GROUP BY time(1h) END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_path_roi_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"roi_actual\") AS count_values, mean(\"roi_actual\") AS mean_roi, min(\"roi_actual\") AS min_roi, max(\"roi_actual\") AS max_roi, percentile(\"roi_actual\", 25) AS p25_roi, percentile(\"roi_actual\", 50) AS p50_roi, percentile(\"roi_actual\", 75) AS p75_roi INTO \"path_roi_aggregates\" FROM \"trade_path\" GROUP BY time(1h) END"
curl -XPOST "http://$INFLUXDB_HOST:$INFLUXDB_PORT/query" --data-urlencode "q=CREATE CONTINUOUS QUERY \"cq_path_roidiff_aggregates\" ON \"$DB_NAME\" BEGIN SELECT count(\"roi_pct_diff\") AS count_values, mean(\"roi_pct_diff\") AS mean_roidiff, min(\"roi_pct_diff\") AS min_roidiff, max(\"roi_pct_diff\") AS max_roidiff, percentile(\"roi_pct_diff\", 25) AS p25_roidiff, percentile(\"roi_pct_diff\", 50) AS p50_roidiff, percentile(\"roi_pct_diff\", 75) AS p75_roidiff INTO \"path_roidiff_aggregates\" FROM \"trade_path\" GROUP BY time(1h) END"

# Output the results
echo "End of script."
