use core::sync::atomic::{AtomicU64, Ordering};

#[repr(align(64))]
#[derive(Default, Debug)]
pub(crate) struct PaddedCounter(pub(crate) AtomicU64);

#[derive(Default, Debug)]
pub(crate) struct Metrics {
    pub(crate) hits: PaddedCounter,
    pub(crate) misses: PaddedCounter,
    pub(crate) insertions: AtomicU64,
    pub(crate) evictions: AtomicU64,
    pub(crate) rejections: AtomicU64,
}

impl Metrics {
    #[inline(always)]
    pub(crate) fn hit(&self) {
        self.hits.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn miss(&self) {
        self.misses.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn insertion(&self) {
        self.insertions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn rejection(&self) {
        self.rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.0.load(Ordering::Relaxed),
            misses: self.misses.0.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub rejections: u64,
}

impl MetricsSnapshot {
    #[inline]
    pub(crate) fn merge(&mut self, other: &MetricsSnapshot) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.insertions += other.insertions;
        self.evictions += other.evictions;
        self.rejections += other.rejections;
    }
}
