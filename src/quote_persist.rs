//! Quote persistence filter: require an improved BBO to survive N later BBO updates
//! before an arbitrage trade may be sent. Suppresses phantom thin-pair flashes.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistVerdict {
    /// N==0, or no pending and this tick did not improve → caller uses raw BBO flags.
    Inactive,
    /// Improvement armed (or still waiting); do not send; log `awaiting_persist`.
    Awaiting,
    /// Survived N BBO updates at a still-good price; may send.
    Ready,
    /// Pending quote worsened/vanished before N confirms; log `persist_failed`.
    Failed,
}

#[derive(Clone, Copy, Debug)]
struct Pending {
    price: f64,
    remaining: u16,
}

/// Per-listener tracker keyed by local pair index.
pub struct QuotePersistTracker {
    required: u16,
    bids: Vec<Option<Pending>>,
    asks: Vec<Option<Pending>>,
}

impl QuotePersistTracker {
    pub fn new(num_pairs: usize, required_updates: u16) -> Self {
        Self {
            required: required_updates,
            bids: vec![None; num_pairs],
            asks: vec![None; num_pairs],
        }
    }

    /// Drop all pending quotes (e.g. on WS reconnect / book reset).
    pub fn clear(&mut self) {
        self.bids.fill(None);
        self.asks.fill(None);
    }

    pub fn observe_bid(&mut self, idx: usize, bid_price: f64, improved: bool) -> PersistVerdict {
        Self::observe_side(
            &mut self.bids,
            self.required,
            idx,
            bid_price,
            improved,
            true,
        )
    }

    pub fn observe_ask(&mut self, idx: usize, ask_price: f64, improved: bool) -> PersistVerdict {
        Self::observe_side(
            &mut self.asks,
            self.required,
            idx,
            ask_price,
            improved,
            false,
        )
    }

    fn still_good(is_bid: bool, current: f64, pending_price: f64) -> bool {
        if is_bid {
            current + 1e-12 >= pending_price
        } else {
            current > 0.0 && current <= pending_price + 1e-12
        }
    }

    fn observe_side(
        slots: &mut [Option<Pending>],
        required: u16,
        idx: usize,
        price: f64,
        improved: bool,
        is_bid: bool,
    ) -> PersistVerdict {
        if required == 0 || idx >= slots.len() {
            return PersistVerdict::Inactive;
        }

        let mut failed = false;
        if let Some(p) = slots[idx] {
            if Self::still_good(is_bid, price, p.price) {
                let better = if is_bid {
                    price > p.price + 1e-12
                } else {
                    price + 1e-12 < p.price
                };
                if improved && better {
                    slots[idx] = Some(Pending {
                        price,
                        remaining: required,
                    });
                    return PersistVerdict::Awaiting;
                }
                let left = p.remaining.saturating_sub(1);
                if left == 0 {
                    slots[idx] = None;
                    return PersistVerdict::Ready;
                }
                slots[idx] = Some(Pending {
                    price: p.price,
                    remaining: left,
                });
                return PersistVerdict::Awaiting;
            } else {
                slots[idx] = None;
                failed = true;
            }
        }

        if improved {
            slots[idx] = Some(Pending {
                price,
                remaining: required,
            });
            return PersistVerdict::Awaiting;
        }
        if failed {
            PersistVerdict::Failed
        } else {
            PersistVerdict::Inactive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_required_always_inactive() {
        let mut t = QuotePersistTracker::new(4, 0);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Inactive);
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Inactive);
    }

    #[test]
    fn one_update_awaits_then_ready() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Ready);
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Inactive);
    }

    #[test]
    fn phantom_bid_fails_before_confirm() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 99.0, false), PersistVerdict::Failed);
    }

    #[test]
    fn ask_side_symmetric() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_ask(3, 50.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_ask(3, 50.0, false), PersistVerdict::Ready);
        assert_eq!(t.observe_ask(3, 51.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_ask(3, 52.0, false), PersistVerdict::Failed);
    }

    #[test]
    fn two_updates_need_two_confirms() {
        let mut t = QuotePersistTracker::new(4, 2);
        assert_eq!(t.observe_bid(2, 10.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.0, false), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.1, false), PersistVerdict::Ready);
    }

    #[test]
    fn better_price_while_awaiting_resets_counter() {
        let mut t = QuotePersistTracker::new(4, 2);
        assert_eq!(t.observe_bid(2, 10.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.5, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.5, false), PersistVerdict::Awaiting);
        assert_eq!(t.observe_bid(2, 10.5, false), PersistVerdict::Ready);
    }

    #[test]
    fn clear_drops_pending_so_confirm_does_not_ready() {
        let mut t = QuotePersistTracker::new(4, 1);
        assert_eq!(t.observe_bid(2, 100.0, true), PersistVerdict::Awaiting);
        assert_eq!(t.observe_ask(3, 50.0, true), PersistVerdict::Awaiting);
        t.clear();
        // Reconnect / book reset must not treat the next tick as a confirmation.
        assert_eq!(t.observe_bid(2, 100.0, false), PersistVerdict::Inactive);
        assert_eq!(t.observe_ask(3, 50.0, false), PersistVerdict::Inactive);
    }
}
