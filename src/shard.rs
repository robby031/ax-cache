use core::borrow::Borrow;
use core::hash::Hash;
use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use axhash_map::AxHashMap;
use hashbrown::hash_map::RawEntryMut;
use parking_lot::RwLock;

use crate::InsertOutcome;
use crate::metrics::Metrics;
use crate::policy::{Policy, bump_freq};
use crate::tinylfu::CountMinSketch;

pub(crate) struct Entry<V> {
    pub(crate) value: V,
    pub(crate) expiry_ms: u32,
    pub(crate) freq: AtomicU8,
}

pub(crate) struct ShardInner<K, V> {
    pub(crate) map: AxHashMap<K, Entry<V>>,
    pub(crate) policy: Policy<K>,
    pub(crate) sketch: CountMinSketch,
}

#[repr(align(64))]
pub(crate) struct Shard<K, V> {
    inner: RwLock<ShardInner<K, V>>,
    capacity: usize,
    size: AtomicUsize,
    pub(crate) metrics: Metrics,
}

impl<K, V> Shard<K, V> {
    pub(crate) fn new(capacity: usize) -> Self {
        let inner = ShardInner {
            map: AxHashMap::with_capacity(capacity),
            policy: Policy::new(capacity),
            sketch: CountMinSketch::new(capacity),
        };
        Self {
            inner: RwLock::new(inner),
            capacity,
            size: AtomicUsize::new(0),
            metrics: Metrics::default(),
        }
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub(crate) fn get<Q>(&self, key: &Q, hash: u64, now_ms: u32) -> Option<V>
    where
        K: Eq + Hash + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let g = self.inner.read();
        let (_, entry) = g.map.raw_entry().from_key_hashed_nocheck(hash, key)?;
        if now_ms >= entry.expiry_ms {
            return None;
        }
        bump_freq(&entry.freq);
        Some(entry.value.clone())
    }

    pub(crate) fn insert(
        &self,
        key: K,
        value: V,
        expiry_ms: u32,
        now_ms: u32,
        key_hash: u64,
    ) -> InsertOutcome
    where
        K: Eq + Hash + Clone,
    {
        let mut g = self.inner.write();
        let cap_full = g.map.len() >= self.capacity;
        let ShardInner {
            map,
            policy,
            sketch,
        } = &mut *g;

        match map.raw_entry_mut().from_key_hashed_nocheck(key_hash, &key) {
            RawEntryMut::Occupied(mut occ) => {
                let entry = occ.get_mut();
                entry.value = value;
                entry.expiry_ms = expiry_ms;
                sketch.increment(key_hash);
                InsertOutcome::Updated
            }
            RawEntryMut::Vacant(vac) => {
                let est = sketch.estimate(key_hash);
                sketch.increment(key_hash);

                if cap_full && est == 0 {
                    self.metrics.rejection();
                    return InsertOutcome::Rejected;
                }

                vac.insert_hashed_nocheck(
                    key_hash,
                    key.clone(),
                    Entry {
                        value,
                        freq: AtomicU8::new(0),
                        expiry_ms,
                    },
                );
                policy.admit_small(key);
                self.metrics.insertion();
                let evicted = Self::rebalance(&mut g, self.capacity, now_ms);
                if evicted > 0 {
                    self.metrics.evictions.fetch_add(evicted, Ordering::Relaxed);
                }
                let new_len = g.map.len();
                drop(g);
                self.size.store(new_len, Ordering::Relaxed);
                InsertOutcome::Inserted
            }
        }
    }

    pub(crate) fn contains_key<Q>(&self, key: &Q, hash: u64, now_ms: u32) -> bool
    where
        K: Eq + Hash + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let g = self.inner.read();
        match g.map.raw_entry().from_key_hashed_nocheck(hash, key) {
            Some((_, entry)) => now_ms < entry.expiry_ms,
            None => false,
        }
    }

    pub(crate) fn clear(&self)
    where
        K: Eq + Hash,
    {
        let mut g = self.inner.write();
        g.map.clear();
        g.policy.small.clear();
        g.policy.main.clear();
        g.policy.stale_estimate = 0;
        g.sketch.reset();
        drop(g);
        self.size.store(0, Ordering::Relaxed);
    }

    pub(crate) fn remove<Q>(&self, key: &Q, hash: u64) -> Option<V>
    where
        K: Eq + Hash + Clone + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut g = self.inner.write();
        let removed = match g.map.raw_entry_mut().from_key_hashed_nocheck(hash, key) {
            RawEntryMut::Occupied(occ) => Some(occ.remove().value),
            RawEntryMut::Vacant(_) => None,
        };
        if removed.is_some() {
            g.policy.mark_stale();
            let ShardInner {
                ref map,
                ref mut policy,
                ..
            } = *g;
            policy.compact(map);
            self.size.store(g.map.len(), Ordering::Relaxed);
        }
        removed
    }

    pub(crate) fn sweep_expired(&self, now_ms: u32, budget: usize)
    where
        K: Eq + Hash + Clone,
    {
        let mut g = self.inner.write();
        let mut swept = 0;
        while swept < budget {
            let Some(k) = g.policy.small.front() else {
                break;
            };
            match g.map.get(k) {
                None => {
                    g.policy.small.pop_front();
                    g.policy.note_stale_popped();
                }
                Some(entry) if now_ms >= entry.expiry_ms => {
                    let k = g.policy.small.pop_front().unwrap();
                    g.map.remove(&k);
                    self.metrics.eviction();
                    swept += 1;
                }
                _ => break,
            }
        }
        while swept < budget {
            let Some(k) = g.policy.main.front() else {
                break;
            };
            match g.map.get(k) {
                None => {
                    g.policy.main.pop_front();
                    g.policy.note_stale_popped();
                }
                Some(entry) if now_ms >= entry.expiry_ms => {
                    let k = g.policy.main.pop_front().unwrap();
                    g.map.remove(&k);
                    self.metrics.eviction();
                    swept += 1;
                }
                _ => break,
            }
        }
        if swept > 0 {
            self.size.store(g.map.len(), Ordering::Relaxed);
        }
    }

    const POLITE_STEP_BUDGET: usize = 8;
    const MAX_REBALANCE_STEPS: usize = 32;

    fn rebalance(g: &mut ShardInner<K, V>, capacity: usize, now_ms: u32) -> u64
    where
        K: Eq + Hash,
    {
        let mut evicted = 0u64;
        let mut steps = 0usize;
        let mut polite_promotes = 0usize;

        while g.map.len() > capacity && steps < Self::MAX_REBALANCE_STEPS {
            let force = polite_promotes >= Self::POLITE_STEP_BUDGET;

            let acted = if g.policy.small.len() > g.policy.small_cap {
                step_small(g, now_ms, force, &mut evicted)
            } else if !g.policy.main.is_empty() {
                step_main(g, now_ms, force, &mut evicted)
            } else if !g.policy.small.is_empty() {
                step_small(g, now_ms, force, &mut evicted)
            } else {
                StepResult::QueueEmpty
            };

            match acted {
                StepResult::QueueEmpty => break,
                StepResult::Dropped => {
                    // Made progress; that's enough for this insert.
                    break;
                }
                StepResult::PromotedOrSkipped => {
                    polite_promotes += 1;
                }
            }
            steps += 1;
        }
        evicted
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StepResult {
    QueueEmpty,
    Dropped,
    PromotedOrSkipped,
}

fn step_small<K, V>(
    g: &mut ShardInner<K, V>,
    now_ms: u32,
    force: bool,
    evicted: &mut u64,
) -> StepResult
where
    K: Eq + Hash,
{
    let Some(k) = g.policy.small.pop_front() else {
        return StepResult::QueueEmpty;
    };
    let action = decide_small(&g.map, &k, now_ms);
    // Under capacity pressure with no natural drop in sight, force eviction
    // of the entry we just popped to guarantee FIFO progress.
    let action = if force && matches!(action, EvictAction::Promote) {
        EvictAction::Drop
    } else {
        action
    };
    match action {
        EvictAction::Promote => {
            g.policy.admit_main(k);
            StepResult::PromotedOrSkipped
        }
        EvictAction::Drop => {
            g.map.remove(&k);
            *evicted += 1;
            StepResult::Dropped
        }
        EvictAction::Skip => {
            g.policy.note_stale_popped();
            StepResult::PromotedOrSkipped
        }
    }
}

fn step_main<K, V>(
    g: &mut ShardInner<K, V>,
    now_ms: u32,
    force: bool,
    evicted: &mut u64,
) -> StepResult
where
    K: Eq + Hash,
{
    let Some(k) = g.policy.main.pop_front() else {
        return StepResult::QueueEmpty;
    };
    let action = decide_main(&g.map, &k, now_ms);
    let action = if force && matches!(action, EvictAction::Promote) {
        EvictAction::Drop
    } else {
        action
    };
    match action {
        EvictAction::Promote => {
            g.policy.admit_main(k);
            StepResult::PromotedOrSkipped
        }
        EvictAction::Drop => {
            g.map.remove(&k);
            *evicted += 1;
            StepResult::Dropped
        }
        EvictAction::Skip => {
            g.policy.note_stale_popped();
            StepResult::PromotedOrSkipped
        }
    }
}

enum EvictAction {
    Promote,
    Drop,
    Skip,
}

fn decide_small<K, V>(map: &AxHashMap<K, Entry<V>>, k: &K, now_ms: u32) -> EvictAction
where
    K: Eq + Hash,
{
    match map.get(k) {
        None => EvictAction::Skip,
        Some(entry) => {
            if now_ms >= entry.expiry_ms {
                return EvictAction::Drop;
            }
            if entry.freq.load(Ordering::Relaxed) > 0 {
                let _ = entry.freq.swap(0, Ordering::Relaxed);
                EvictAction::Promote
            } else {
                EvictAction::Drop
            }
        }
    }
}

fn decide_main<K, V>(map: &AxHashMap<K, Entry<V>>, k: &K, now_ms: u32) -> EvictAction
where
    K: Eq + Hash,
{
    match map.get(k) {
        None => EvictAction::Skip,
        Some(entry) => {
            if now_ms >= entry.expiry_ms {
                return EvictAction::Drop;
            }
            let f = entry.freq.load(Ordering::Relaxed);
            if f > 0 {
                entry.freq.fetch_sub(1, Ordering::Relaxed);
                EvictAction::Promote
            } else {
                EvictAction::Drop
            }
        }
    }
}
