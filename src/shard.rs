use core::borrow::Borrow;
use core::hash::Hash;
use core::sync::atomic::{AtomicU8, Ordering};

use axhash_map::AxHashMap;
use parking_lot::RwLock;

use crate::metrics::Metrics;
use crate::policy::{Policy, bump_freq};
use crate::tinylfu::CountMinSketch;

pub(crate) const NO_EXPIRY: u32 = u32::MAX;

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
            metrics: Metrics::default(),
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.inner.read().map.len()
    }

    pub(crate) fn get<Q>(&self, key: &Q, now_ms: u32) -> Option<V>
    where
        K: Eq + Hash + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let g = self.inner.read();
        let entry = g.map.get(key)?;
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
    ) -> bool
    where
        K: Eq + Hash + Clone,
    {
        let mut g = self.inner.write();
        g.sketch.increment(key_hash);

        if let Some(entry) = g.map.get_mut(&key) {
            entry.value = value;
            entry.expiry_ms = expiry_ms;
            return false;
        }

        if g.map.len() >= self.capacity && g.sketch.estimate(key_hash) <= 1 {
            self.metrics.rejection();
            return true;
        }

        g.map.insert(
            key.clone(),
            Entry {
                value,
                freq: AtomicU8::new(0),
                expiry_ms,
            },
        );
        g.policy.admit_small(key);
        self.metrics.insertion();
        Self::rebalance(&mut g, self.capacity, now_ms, &self.metrics);
        true
    }

    pub(crate) fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Eq + Hash + Clone + Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut g = self.inner.write();
        let removed = g.map.remove(key).map(|e| e.value);
        if removed.is_some() {
            g.policy.mark_stale();
            let ShardInner {
                ref map,
                ref mut policy,
                ..
            } = *g;
            policy.compact(map);
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
    }

    fn rebalance(g: &mut ShardInner<K, V>, capacity: usize, now_ms: u32, metrics: &Metrics)
    where
        K: Eq + Hash,
    {
        while g.map.len() > capacity {
            let prefer_small = g.policy.small.len() > g.policy.small_cap;
            let stepped = if prefer_small && !g.policy.small.is_empty() {
                step_small(g, now_ms, metrics)
            } else if !g.policy.main.is_empty() {
                step_main(g, now_ms, metrics)
            } else if !g.policy.small.is_empty() {
                step_small(g, now_ms, metrics)
            } else {
                false
            };
            if !stepped {
                break;
            }
        }
    }
}

fn step_small<K, V>(g: &mut ShardInner<K, V>, now_ms: u32, metrics: &Metrics) -> bool
where
    K: Eq + Hash,
{
    let Some(k) = g.policy.small.pop_front() else {
        return false;
    };
    match decide_small(&g.map, &k, now_ms) {
        EvictAction::Promote => g.policy.admit_main(k),
        EvictAction::Drop => {
            g.map.remove(&k);
            metrics.eviction();
        }
        EvictAction::Skip => g.policy.note_stale_popped(),
    }
    true
}

fn step_main<K, V>(g: &mut ShardInner<K, V>, now_ms: u32, metrics: &Metrics) -> bool
where
    K: Eq + Hash,
{
    let Some(k) = g.policy.main.pop_front() else {
        return false;
    };
    match decide_main(&g.map, &k, now_ms) {
        EvictAction::Promote => g.policy.admit_main(k),
        EvictAction::Drop => {
            g.map.remove(&k);
            metrics.eviction();
        }
        EvictAction::Skip => g.policy.note_stale_popped(),
    }
    true
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
                entry.freq.store(0, Ordering::Relaxed);
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
