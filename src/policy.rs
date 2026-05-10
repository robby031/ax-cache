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

    #[inline(always)]
    pub(crate) fn mark_stale(&mut self) {
        self.stale_estimate = self.stale_estimate.saturating_add(1);
    }

    #[inline(always)]
    pub(crate) fn note_stale_popped(&mut self) {
        self.stale_estimate = self.stale_estimate.saturating_sub(1);
    }

    pub(crate) fn compact<V>(&mut self, map: &axhash_map::HashMap<K, super::shard::Entry<V>>)
    where
        K: Eq + core::hash::Hash,
    {
        let threshold = self.capacity.saturating_mul(2);
        let total = self.small.len() + self.main.len();
        if total <= threshold {
            return;
        }
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
    let cur = freq.load(Ordering::Relaxed);
    if cur < FREQ_MAX {
        freq.store(cur + 1, Ordering::Relaxed);
    }
}
