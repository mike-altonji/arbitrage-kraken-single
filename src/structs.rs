#[derive(Clone)]
pub struct PairData {
    pub bid_price: f64,
    pub ask_price: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub order_min: f64,
    pub cost_min: f64,
    pub price_decimals: usize,
    pub volume_decimals: usize,
    pub pair_status: bool,
    pub kraken_ts: f64,
}
pub type PairDataVec = Vec<PairData>;

/// Payload of the trading channel: either a two-pair arbitrage or a
/// same-pair momentum round trip. Maker mode bypasses this channel and uses
/// the desired-quote registry in `maker.rs`.
#[derive(Clone)]
pub enum TradeCommand {
    Arb(OrderInfo),
    Momentum(MomentumOrder),
}

/// Same-pair round trip: limit IOC buy at `limit_buy_price`, hold `hold_ms`,
/// then market sell the filled volume on the same pair.
#[derive(Clone)]
pub struct MomentumOrder {
    /// Pair we buy and sell (Y).
    pub pair_name: &'static str,
    /// Sibling pair whose bid jump triggered this trade (X). Telemetry only.
    pub trigger_pair_name: &'static str,
    pub volume_coin: f64,
    pub volume_decimals_coin: usize,
    /// Y's ask at evaluation time — the limit IOC buy price.
    pub limit_buy_price: f64,
    pub price_decimals: usize,
    /// Sampled log-uniformly in [1, 1000] ms; logged for experimentation.
    pub hold_ms: f64,
    /// Gap between X's fx-converted bid and Y's ask, in bps of Y's ask.
    pub gap_bps: f64,
    pub send_timestamp: u128,
    pub updated_pair_kraken_ts: f64,
    pub opportunity_id: u64,
}

#[derive(Clone)]
#[allow(dead_code)] // Stablecoin legs unused while we skip trading back to stables
pub struct OrderInfo {
    pub pair1_name: &'static str,
    pub pair2_name: &'static str,
    pub pair1_stable_name: &'static str,
    pub pair2_stable_name: &'static str,
    pub volume_coin: f64,
    pub volume_stable: f64,
    pub volume_decimals_coin: usize,
    pub volume_decimals_stable: usize,
    pub send_timestamp: u128,
    pub pair1_price: f64,
    pub price_decimals: usize,
    pub updated_pair_kraken_ts: f64,
    /// Links trade execution forensics back to the evaluation that created this order.
    pub opportunity_id: u64,
    pub planned_vwap_ask: f64,
    pub planned_vwap_bid: f64,
    /// `pair1_stable.bid / pair2_stable.ask` at eval time — converts pair2 quote → pair1 quote.
    pub quote2_to_quote1_fx: f64,
}
