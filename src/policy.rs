use core::sync::atomic::{AtomicU8, Ordering};
use std::collections::VecDeque;

pub(crate) const FREQ_MAX: u8 = 3;

pub(crate) struct Policy<K> {
    pub(crate) small: VecDeque<K>,
    pub(crate) main: VecDeque<K>,
    pub(crate) small_cap: usize,
    pub(crate) stale_estimate: usize,
    pub(crate) capacity: usize,
}

impl<K> Policy<K> {
    pub(crate) fn new(capacity: usize) -> Self {
        let small_cap = (capacity / 10).max(1);
        let main_cap = capacity.saturating_sub(small_cap).max(1);
        Self {
            small: VecDeque::with_capacity(small_cap.saturating_add(1)),
            main: VecDeque::with_capacity(main_cap.saturating_add(1)),
            small_cap,
            stale_estimate: 0,
            capacity,
        }
    }

    #[inline(always)]
    pub(crate) fn admit_small(&mut self, key: K) {
        self.small.push_back(key);
    }

    #[inline(always)]
    pub(crate) fn admit_main(&mut self, key: K) {
        self.main.push_back(key);
    }

    // Called when an entry is removed from the map but its key remains
    // somewhere in the queues. Bumps the stale counter so we can trigger
    // compaction before the queues grow without bound.
    #[inline(always)]
    pub(crate) fn mark_stale(&mut self) {
        self.stale_estimate = self.stale_estimate.saturating_add(1);
    }

    // Called when a stale pop (Skip) is observed during rebalance.
    #[inline(always)]
    pub(crate) fn note_stale_popped(&mut self) {
        self.stale_estimate = self.stale_estimate.saturating_sub(1);
    }

    // Drain stale entries from the front of both queues. Called
    // when the total queue length exceeds "2 × capacity" to bound
    // memory growth from insert/remove churn without rebalance.
    pub(crate) fn compact<V, S: core::hash::BuildHasher>(
        &mut self,
        map: &hashbrown::HashMap<K, super::shard::Entry<V>, S>,
    ) where
        K: Eq + core::hash::Hash,
    {
        let threshold = self.capacity.saturating_mul(2);
        let total = self.small.len() + self.main.len();
        if total <= threshold {
            return;
        }
        // Drain up to `stale estimate` entries from the fronts.
        let mut budget = self.stale_estimate;
        while budget > 0 {
            if let Some(front) = self.small.front()
                && !map.contains_key(front)
            {
                self.small.pop_front();
                budget -= 1;
                continue;
            }
            if let Some(front) = self.main.front()
                && !map.contains_key(front)
            {
                self.main.pop_front();
                budget -= 1;
                continue;
            }
            break;
        }
        self.stale_estimate = self
            .stale_estimate
            .saturating_sub(self.stale_estimate.saturating_sub(budget));
    }
}

#[inline(always)]
pub(crate) fn bump_freq(freq: &AtomicU8) {
    // Relaxed store instead of CAS loop: the benign data race (two threads
    // reading the same cur and both storing cur + 1 can lose at most
    // one increment, but since we saturate at freq_max and eviction only
    // checks >0, this is harmless. Avoids CAS retry storms on hot keys.
    let cur = freq.load(Ordering::Relaxed);
    if cur < FREQ_MAX {
        freq.store(cur + 1, Ordering::Relaxed);
    }
}
