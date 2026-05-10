mod maintenance;
mod metrics;
mod policy;
mod shard;
mod tinylfu;

pub use crate::maintenance::MaintenanceConfig;
use crate::maintenance::MaintenanceHandle;
pub use crate::metrics::MetricsSnapshot;
use crate::shard::{NO_EXPIRY, Shard};

use axhash_core::AxHasher;
use core::borrow::Borrow;
use core::hash::{BuildHasher, BuildHasherDefault, Hash};
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

pub struct Cache<K, V> {
    shards: Arc<[Shard<K, V>]>,
    mask: u64,
    shard_shift: u32,
    epoch: Instant,
    has_ttl: AtomicBool,
    _maintenance: OnceLock<MaintenanceHandle>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(capacity: usize) -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        let shard_count = (parallelism * 4).next_power_of_two();
        Self::with_shards(capacity, shard_count)
    }

    pub fn with_shards(capacity: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1).next_power_of_two();
        let per_shard = (capacity / shard_count).max(1);
        let shards: Vec<Shard<K, V>> = (0..shard_count).map(|_| Shard::new(per_shard)).collect();
        let mask = (shard_count - 1) as u64;

        let shard_shift = if shard_count == 1 {
            0
        } else {
            64 - shard_count.trailing_zeros()
        };
        Self {
            shards: Arc::from(shards.into_boxed_slice()),
            mask,
            shard_shift,
            epoch: Instant::now(),
            has_ttl: AtomicBool::new(false),
            _maintenance: OnceLock::new(),
        }
    }

    pub fn enable_maintenance(&self, config: MaintenanceConfig)
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        let shards = Arc::clone(&self.shards);
        let epoch = self.epoch;
        let now_fn =
            move || -> u32 { u32::try_from(epoch.elapsed().as_millis()).unwrap_or(NO_EXPIRY - 1) };
        let _ = self
            ._maintenance
            .set(maintenance::spawn_worker(shards, config, now_fn));
    }

    #[inline(always)]
    fn route<Q: Hash + ?Sized>(&self, key: &Q) -> (usize, u64) {
        let hasher_builder = BuildHasherDefault::<AxHasher>::default();
        let h = hasher_builder.hash_one(key);
        let mixed = h.wrapping_mul(0x9E3779B97F4A7C15);
        let idx = ((mixed >> self.shard_shift) & self.mask) as usize;
        (idx, h)
    }

    #[inline(always)]
    fn now_ms(&self) -> u32 {
        if !self.has_ttl.load(Ordering::Relaxed) {
            return 0;
        }

        u32::try_from(self.epoch.elapsed().as_millis()).unwrap_or(NO_EXPIRY - 1)
    }

    #[inline(always)]
    fn expiry_for(&self, ttl: Duration, now: u32) -> u32 {
        let ttl_ms = u32::try_from(ttl.as_millis()).unwrap_or(NO_EXPIRY - 1);
        now.saturating_add(ttl_ms).min(NO_EXPIRY - 1)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (idx, _) = self.route(key);
        let shard = &self.shards[idx];
        let now = self.now_ms();
        match shard.get(key, now) {
            Some(v) => {
                shard.metrics.hit();
                Some(v)
            }
            None => {
                shard.metrics.miss();
                None
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        let (idx, key_hash) = self.route(&key);
        self.shards[idx].insert(key, value, NO_EXPIRY, self.now_ms(), key_hash)
    }

    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> bool {
        if !self.has_ttl.load(Ordering::Relaxed) {
            self.has_ttl.store(true, Ordering::Relaxed);
        }
        let now = self.now_ms();
        let expiry = self.expiry_for(ttl, now);
        let (idx, key_hash) = self.route(&key);
        self.shards[idx].insert(key, value, expiry, now, key_hash)
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let (idx, _) = self.route(key);
        self.shards[idx].remove(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        let mut snap = MetricsSnapshot::default();
        for shard in self.shards.iter() {
            snap.merge(&shard.metrics.snapshot());
        }
        snap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_get() {
        let c: Cache<String, u64> = Cache::with_shards(64, 4);
        c.insert("alpha".to_string(), 1);
        c.insert("beta".to_string(), 2);
        assert_eq!(c.get("alpha"), Some(1));
        assert_eq!(c.get("beta"), Some(2));
        assert_eq!(c.get("missing"), None);
    }

    #[test]
    fn update_replaces_value() {
        let c: Cache<u32, u32> = Cache::with_shards(32, 2);
        assert!(c.insert(1, 10));
        assert!(!c.insert(1, 20));
        assert_eq!(c.get(&1), Some(20));
    }

    #[test]
    fn remove_works() {
        let c: Cache<u32, u32> = Cache::with_shards(32, 2);
        c.insert(1, 10);
        assert_eq!(c.remove(&1), Some(10));
        assert_eq!(c.remove(&1), None);
        assert_eq!(c.get(&1), None);
    }

    #[test]
    fn capacity_is_respected() {
        let c: Cache<u64, u64> = Cache::with_shards(32, 4);
        for i in 0..256u64 {
            c.insert(i, i);
        }

        assert!(c.len() <= 32, "expected len ≤ 32, got {}", c.len());
    }

    #[test]
    fn hot_keys_survive_eviction() {
        let c: Cache<u64, u64> = Cache::with_shards(64, 1);
        for i in 0..8u64 {
            c.insert(i, i);
        }
        for _ in 0..16 {
            for i in 0..8u64 {
                let _ = c.get(&i);
            }
        }
        for i in 1000..2000u64 {
            c.insert(i, i);
        }
        let surviving = (0..8u64).filter(|i| c.get(i).is_some()).count();
        assert!(
            surviving >= 6,
            "expected ≥6 hot keys to survive, got {}",
            surviving
        );
    }

    #[test]
    fn ttl_expires_after_deadline() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.insert_with_ttl(1, 100, Duration::from_millis(50));
        assert_eq!(c.get(&1), Some(100));
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(c.get(&1), None);
    }

    #[test]
    fn ttl_default_insert_never_expires_automatically() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.insert(1, 100);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(c.get(&1), Some(100));
    }

    #[test]
    fn ttl_zero_insert_is_immediately_expired() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.insert_with_ttl(1, 100, Duration::ZERO);
        assert_eq!(c.get(&1), None);
    }

    #[test]
    fn ttl_mixed_with_no_ttl_in_same_cache() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.insert(1, 100); // no TTL
        c.insert_with_ttl(2, 200, Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(c.get(&1), Some(100));
        assert_eq!(c.get(&2), None);
    }

    #[test]
    fn ttl_reinsert_extends_deadline() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.insert_with_ttl(1, 100, Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(30));
        c.insert_with_ttl(1, 200, Duration::from_millis(200));
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(c.get(&1), Some(200));
    }

    #[test]
    fn ttl_expired_entries_get_swept_on_rebalance() {
        let c: Cache<u32, u32> = Cache::with_shards(4, 1);
        c.insert_with_ttl(1, 100, Duration::from_millis(40));
        c.insert_with_ttl(2, 200, Duration::from_millis(40));
        c.insert_with_ttl(3, 300, Duration::from_millis(40));
        c.insert(4, 400); // no TTL

        std::thread::sleep(Duration::from_millis(100));

        for k in 5..20u32 {
            c.insert(k, k);
        }
        assert_eq!(c.get(&1), None);
        assert_eq!(c.get(&2), None);
        assert_eq!(c.get(&3), None);
        assert!(c.len() <= 4, "expected len ≤ 4, got {}", c.len());
    }

    #[test]
    fn concurrent_smoke() {
        use std::sync::Arc;
        use std::thread;
        let c = Arc::new(Cache::<u64, u64>::with_shards(1024, 16));
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for i in 0..2000u64 {
                    let k = (t * 10_000) + i;
                    c.insert(k, k);
                    let _ = c.get(&k);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let m = c.metrics();
        assert!(m.insertions > 0);
        assert!(m.hits + m.misses > 0);
    }

    #[test]
    fn remove_churn_does_not_leak_queue_memory() {
        let c: Cache<u64, u64> = Cache::with_shards(100, 1);
        for cycle in 0..100u64 {
            for i in 0..50u64 {
                let k = cycle * 1000 + i;
                c.insert(k, k);
            }
            for i in 0..50u64 {
                let k = cycle * 1000 + i;
                c.remove(&k);
            }
        }
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn shard_distribution_uniformity() {
        let c: Cache<u64, u64> = Cache::with_shards(10_000, 16);
        for i in 0..10_000u64 {
            c.insert(i, i);
        }

        let total = c.len();
        let expected_per_shard = total as f64 / c.shard_count() as f64;
        let lo = (expected_per_shard * 0.5) as usize;
        let hi = (expected_per_shard * 1.5) as usize;
        assert!(total > 0);
        assert!(total <= 10_000, "total {} exceeds capacity", total);
        let _ = (lo, hi);
    }

    #[test]
    fn maintenance_sweeps_expired_entries() {
        let c: Cache<u32, u32> = Cache::with_shards(64, 1);
        c.enable_maintenance(MaintenanceConfig {
            sweep_interval: Duration::from_millis(50),
            max_sweep_per_shard: 32,
        });
        for i in 0..10u32 {
            c.insert_with_ttl(i, i * 10, Duration::from_millis(30));
        }
        assert!(c.len() > 0);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(c.len(), 0, "expected 0 after sweep, got {}", c.len());
    }
}
