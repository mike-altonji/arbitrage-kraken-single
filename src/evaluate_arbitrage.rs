use crate::structs::PairDataVec;

pub fn evaluate_arbitrage(pair_data_vec: &mut PairDataVec, idx: usize) {
    let pair_data = pair_data_vec.get(idx).unwrap();
    let bid_price = pair_data.bid_price;
    let ask_price = pair_data.ask_price;
    let bid_volume = pair_data.bid_volume;
    let ask_volume = pair_data.ask_volume;
}
