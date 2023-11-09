### Running Instructions
1. Run `brew services start influxdb@1` if on Mac. This starts the InfluxDB server.
2. Run `./scripts/setup_influxdb.sh` to initialize InfluxDB, if needed.
3. `cargo run`

### Building
```
cargo run --release
./target/release/arbitrage
```