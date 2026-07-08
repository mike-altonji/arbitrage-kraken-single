use crate::structs::PairData;

pub const BOOK_DEPTH: usize = 10;
pub const CHECKSUM_DEPTH: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BboChange {
    pub changed: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct PriceLevel {
    pub price_str: [u8; 32],
    pub price_len: u8,
    pub vol_str: [u8; 32],
    pub vol_len: u8,
    pub price: f64,
    pub volume: f64,
    #[allow(dead_code)] // Per-level timestamp; used for book integrity / future depth walking
    pub ts: f64,
}

impl Default for PriceLevel {
    fn default() -> Self {
        Self {
            price_str: [0u8; 32],
            price_len: 0,
            vol_str: [0u8; 32],
            vol_len: 0,
            price: 0.0,
            volume: 0.0,
            ts: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    pub bids: [PriceLevel; BOOK_DEPTH],
    pub asks: [PriceLevel; BOOK_DEPTH],
    pub bid_count: u8,
    pub ask_count: u8,
    pub ready: bool,
}

pub type OrderBookVec = Vec<OrderBook>;

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: [PriceLevel::default(); BOOK_DEPTH],
            asks: [PriceLevel::default(); BOOK_DEPTH],
            bid_count: 0,
            ask_count: 0,
            ready: false,
        }
    }

    pub fn reset(&mut self) {
        self.bids = [PriceLevel::default(); BOOK_DEPTH];
        self.asks = [PriceLevel::default(); BOOK_DEPTH];
        self.bid_count = 0;
        self.ask_count = 0;
        self.ready = false;
    }

    /// Apply a snapshot (`as` / `bs`). Returns max timestamp across all levels.
    pub fn apply_snapshot(
        &mut self,
        asks: &[(&str, &str, f64)],
        bids: &[(&str, &str, f64)],
    ) -> f64 {
        self.ask_count = 0;
        self.bid_count = 0;
        let mut max_ts = 0.0_f64;

        for &(price, volume, ts) in asks.iter().take(BOOK_DEPTH) {
            if let Some(level) = self.asks.get_mut(self.ask_count as usize) {
                *level = make_level(price, volume, ts);
                self.ask_count += 1;
                max_ts = max_ts.max(ts);
            }
        }

        for &(price, volume, ts) in bids.iter().take(BOOK_DEPTH) {
            if let Some(level) = self.bids.get_mut(self.bid_count as usize) {
                *level = make_level(price, volume, ts);
                self.bid_count += 1;
                max_ts = max_ts.max(ts);
            }
        }

        self.ready = true;
        max_ts
    }

    /// Apply ask/bid updates using string-key matching, re-sort, and truncate.
    pub fn apply_updates(
        &mut self,
        ask_updates: &[(&str, &str, f64)],
        bid_updates: &[(&str, &str, f64)],
    ) -> f64 {
        let mut max_ts = 0.0_f64;
        let mut asks = side_to_vec(&self.asks, self.ask_count);
        let mut bids = side_to_vec(&self.bids, self.bid_count);

        for updates in [ask_updates, bid_updates] {
            for &(_, _, ts) in updates {
                max_ts = max_ts.max(ts);
            }
        }

        apply_side_vec(&mut asks, ask_updates, true);
        apply_side_vec(&mut bids, bid_updates, false);

        self.ask_count = vec_to_side(&asks, &mut self.asks);
        self.bid_count = vec_to_side(&bids, &mut self.bids);
        max_ts
    }

    /// Apply incremental updates for one side. Returns max timestamp among applied levels.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn apply_side_updates(&mut self, is_ask: bool, updates: &[(&str, &str, f64)]) -> f64 {
        if is_ask {
            self.apply_updates(updates, &[])
        } else {
            self.apply_updates(&[], updates)
        }
    }

    /// Copy best bid/ask into PairData. If BBO changed, set kraken_ts to msg_ts.
    pub fn sync_bbo(&self, pair_data: &mut PairData, msg_ts: f64) -> BboChange {
        let (new_bid_price, new_bid_volume) = best_bid(self);
        let (new_ask_price, new_ask_volume) = best_ask(self);

        let changed = new_bid_price != pair_data.bid_price
            || new_ask_price != pair_data.ask_price
            || new_bid_volume != pair_data.bid_volume
            || new_ask_volume != pair_data.ask_volume;

        if changed {
            pair_data.bid_price = new_bid_price;
            pair_data.ask_price = new_ask_price;
            pair_data.bid_volume = new_bid_volume;
            pair_data.ask_volume = new_ask_volume;
            pair_data.kraken_ts = msg_ts;
        }

        BboChange { changed }
    }

    /// CRC32 checksum over top 10 levels per Kraken v1 spec.
    pub fn compute_checksum(&self) -> u32 {
        let mut buf = [0u8; 2048];
        let mut pos = 0usize;

        let ask_levels = self.ask_count.min(CHECKSUM_DEPTH as u8) as usize;
        for i in 0..ask_levels {
            pos = append_level_checksum(
                &mut buf,
                pos,
                &self.asks[i].price_str,
                self.asks[i].price_len,
                &self.asks[i].vol_str,
                self.asks[i].vol_len,
            );
        }

        let bid_levels = self.bid_count.min(CHECKSUM_DEPTH as u8) as usize;
        for i in 0..bid_levels {
            pos = append_level_checksum(
                &mut buf,
                pos,
                &self.bids[i].price_str,
                self.bids[i].price_len,
                &self.bids[i].vol_str,
                self.bids[i].vol_len,
            );
        }

        crc32fast::hash(&buf[..pos])
    }
}

fn make_level(price_str: &str, volume_str: &str, ts: f64) -> PriceLevel {
    let price = price_str.parse::<f64>().unwrap_or(0.0);
    let volume = volume_str.parse::<f64>().unwrap_or(0.0);
    let mut level = PriceLevel {
        price,
        volume,
        ts,
        ..PriceLevel::default()
    };
    level.price_len = copy_str_to_buf(&mut level.price_str, price_str);
    level.vol_len = copy_str_to_buf(&mut level.vol_str, volume_str);
    level
}

fn copy_str_to_buf(buf: &mut [u8; 32], s: &str) -> u8 {
    let len = s.len().min(buf.len());
    buf[..len].copy_from_slice(&s.as_bytes()[..len]);
    len as u8
}

fn side_to_vec(levels: &[PriceLevel], count: u8) -> Vec<(String, String, f64)> {
    (0..count as usize)
        .map(|i| {
            let l = &levels[i];
            (
                std::str::from_utf8(&l.price_str[..l.price_len as usize])
                    .unwrap_or("")
                    .to_string(),
                std::str::from_utf8(&l.vol_str[..l.vol_len as usize])
                    .unwrap_or("")
                    .to_string(),
                l.ts,
            )
        })
        .collect()
}

fn vec_to_side(vec: &[(String, String, f64)], dest: &mut [PriceLevel; BOOK_DEPTH]) -> u8 {
    let count = vec.len().min(BOOK_DEPTH);
    for i in 0..count {
        let (p, v, ts) = &vec[i];
        dest[i] = make_level(p, v, *ts);
    }
    for i in count..BOOK_DEPTH {
        dest[i] = PriceLevel::default();
    }
    count as u8
}

fn apply_side_vec(
    levels: &mut Vec<(String, String, f64)>,
    updates: &[(&str, &str, f64)],
    is_ask: bool,
) {
    for &(price, vol, ts) in updates {
        let vol_f: f64 = vol.parse().unwrap_or(0.0);
        if vol_f == 0.0 {
            levels.retain(|(p, _, _)| p != price);
        } else if let Some(idx) = levels.iter().position(|(p, _, _)| p == price) {
            levels[idx] = (price.to_string(), vol.to_string(), ts);
        } else {
            levels.push((price.to_string(), vol.to_string(), ts));
        }
        if is_ask {
            levels.sort_by(|a, b| {
                a.0.parse::<f64>()
                    .unwrap_or(0.0)
                    .partial_cmp(&b.0.parse::<f64>().unwrap_or(0.0))
                    .unwrap()
            });
        } else {
            levels.sort_by(|a, b| {
                b.0.parse::<f64>()
                    .unwrap_or(0.0)
                    .partial_cmp(&a.0.parse::<f64>().unwrap_or(0.0))
                    .unwrap()
            });
        }
        levels.truncate(BOOK_DEPTH);
    }
}

fn best_bid(book: &OrderBook) -> (f64, f64) {
    if book.bid_count == 0 {
        return (0.0, 0.0);
    }
    let level = &book.bids[0];
    (level.price, level.volume)
}

fn best_ask(book: &OrderBook) -> (f64, f64) {
    if book.ask_count == 0 {
        return (0.0, 0.0);
    }
    let level = &book.asks[0];
    (level.price, level.volume)
}

fn append_level_checksum(
    buf: &mut [u8],
    mut pos: usize,
    price_str: &[u8],
    price_len: u8,
    vol_str: &[u8],
    vol_len: u8,
) -> usize {
    pos = append_formatted_field(buf, pos, &price_str[..price_len as usize]);
    append_formatted_field(buf, pos, &vol_str[..vol_len as usize])
}

fn append_formatted_field(buf: &mut [u8], mut pos: usize, field: &[u8]) -> usize {
    let mut started = false;
    for &byte in field {
        if byte == b'.' {
            continue;
        }
        if byte == b'0' && !started {
            continue;
        }
        started = true;
        if pos < buf.len() {
            buf[pos] = byte;
            pos += 1;
        }
    }
    pos
}

pub fn init_order_book_vec(len: usize) -> OrderBookVec {
    (0..len).map(|_| OrderBook::new()).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::excessive_precision)]

    use super::*;

    fn kraken_doc_snapshot() -> OrderBook {
        let asks = [
            ("0.05005", "0.00000500", 1582905487.684110),
            ("0.05010", "0.00000500", 1582905486.187983),
            ("0.05015", "0.00000500", 1582905484.480241),
            ("0.05020", "0.00000500", 1582905486.645658),
            ("0.05025", "0.00000500", 1582905486.859009),
            ("0.05030", "0.00000500", 1582905488.601486),
            ("0.05035", "0.00000500", 1582905488.357312),
            ("0.05040", "0.00000500", 1582905488.785484),
            ("0.05045", "0.00000500", 1582905485.302661),
            ("0.05050", "0.00000500", 1582905486.157467),
        ];
        let bids = [
            ("0.05000", "0.00000500", 1582905487.439814),
            ("0.04995", "0.00000500", 1582905485.119396),
            ("0.04990", "0.00000500", 1582905486.432052),
            ("0.04980", "0.00000500", 1582905480.609351),
            ("0.04975", "0.00000500", 1582905476.793880),
            ("0.04970", "0.00000500", 1582905486.767461),
            ("0.04965", "0.00000500", 1582905481.767528),
            ("0.04960", "0.00000500", 1582905487.378907),
            ("0.04955", "0.00000500", 1582905483.626664),
            ("0.04950", "0.00000500", 1582905488.509872),
        ];

        let mut book = OrderBook::new();
        book.apply_snapshot(&asks, &bids);
        book
    }

    #[test]
    fn checksum_matches_kraken_v1_example() {
        let book = kraken_doc_snapshot();
        assert_eq!(book.compute_checksum(), 974_947_235);
    }

    #[test]
    fn sync_bbo_sets_kraken_ts_on_change() {
        let book = kraken_doc_snapshot();
        let mut pair_data = PairData {
            bid_price: 0.0,
            ask_price: 0.0,
            bid_volume: 0.0,
            ask_volume: 0.0,
            order_min: 0.0,
            cost_min: 0.0,
            price_decimals: 5,
            volume_decimals: 8,
            pair_status: true,
            kraken_ts: 0.0,
        };

        let change = book.sync_bbo(&mut pair_data, 1234.5);
        assert!(change.changed);
        assert_eq!(pair_data.bid_price, 0.05000);
        assert_eq!(pair_data.ask_price, 0.05005);
        assert_eq!(pair_data.kraken_ts, 1234.5);

        let change = book.sync_bbo(&mut pair_data, 9999.0);
        assert!(!change.changed);
        assert_eq!(pair_data.kraken_ts, 1234.5);
    }

    #[test]
    fn delete_ask_promotes_next_level() {
        let mut book = kraken_doc_snapshot();
        book.apply_side_updates(true, &[("0.05005", "0.00000000", 100.0)]);
        assert_eq!(book.asks[0].price, 0.05010);
    }
}
