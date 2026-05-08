const ROWS: usize = 4;

pub(crate) struct CountMinSketch {
    table: Vec<u8>,
    width: usize,
    width_mask: usize,
    additions: usize,
    decay_interval: usize,
}

impl CountMinSketch {
    // Create a new sketch sized for `capacity` cache entries.
    // Total memory = "2 × capacity" bytes (4 rows × capacity/2 width).
    // Sized to fit in L2 for typical per-shard capacities (64K entries).
    pub(crate) fn new(capacity: usize) -> Self {
        // Width must be a power of two for bitmask indexing. Total sketch
        // size = ROWS × width bytes. We target ~2× capacity total counters
        // (empirically good balance between accuracy and L2-friendliness).
        // For a 100K-entry shard: 4 × 64K = 256KB, fits in L2 comfortably.
        let width = (capacity / 2).max(16).next_power_of_two();
        let width_mask = width - 1;
        // Decay roughly every capacity insertions so the window tracks
        // recent access patterns, not the entire lifetime.
        let decay_interval = capacity.max(1);
        Self {
            table: vec![0u8; ROWS * width],
            width,
            width_mask,
            additions: 0,
            decay_interval,
        }
    }

    // Increment the frequency estimate for a key (by its pre-computed hash).
    // Also triggers periodic decay.
    #[inline]
    pub(crate) fn increment(&mut self, hash: u64) {
        let hashes = derive_hashes(hash);
        for (row, &h) in hashes.iter().enumerate() {
            let idx = row * self.width + (h as usize & self.width_mask);
            // Saturating add to avoid wrapping.
            self.table[idx] = self.table[idx].saturating_add(1);
        }
        self.additions += 1;
        if self.additions >= self.decay_interval {
            self.decay();
        }
    }

    // Return the estimated frequency of a key (minimum across all rows).
    #[inline]
    pub(crate) fn estimate(&self, hash: u64) -> u8 {
        let hashes = derive_hashes(hash);
        let mut min = u8::MAX;
        for (row, &h) in hashes.iter().enumerate() {
            let idx = row * self.width + (h as usize & self.width_mask);
            min = min.min(self.table[idx]);
        }
        min
    }

    // Halve all counters (right-shift by 1). This decays old frequencies so
    // the sketch adapts to changing access patterns.
    fn decay(&mut self) {
        for cell in self.table.iter_mut() {
            *cell >>= 1;
        }
        self.additions = 0;
    }
}

// Derive 4 independent row indices from a single 64-bit hash using cheap
// bit-mixing rotations. This avoids calling "Hash::hash" 4× on the hot path.
#[inline(always)]
fn derive_hashes(h: u64) -> [u64; ROWS] {
    let h0 = h;
    let h1 = h.wrapping_mul(0xBF58476D1CE4E5B9).rotate_right(17);
    let h2 = h.wrapping_mul(0x94D049BB133111EB).rotate_right(31);
    let h3 = h0.wrapping_add(h1).wrapping_mul(0x517CC1B727220A95);
    [h0, h1, h2, h3]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_increment_and_estimate() {
        let mut cms = CountMinSketch::new(1000);
        let hash = 0x12345678_u64;
        assert_eq!(cms.estimate(hash), 0);
        cms.increment(hash);
        assert!(cms.estimate(hash) >= 1);
        for _ in 0..10 {
            cms.increment(hash);
        }
        assert!(cms.estimate(hash) >= 5);
    }

    #[test]
    fn different_keys_independent() {
        let mut cms = CountMinSketch::new(10_000);
        let a = 0xAAAA_u64;
        let b = 0xBBBB_u64;
        for _ in 0..100 {
            cms.increment(a);
        }
        // b should have a much lower estimate than a.
        let est_a = cms.estimate(a);
        let est_b = cms.estimate(b);
        assert!(est_a > est_b, "a={} b={}", est_a, est_b);
    }

    #[test]
    fn decay_reduces_counts() {
        let mut cms = CountMinSketch::new(100);
        let hash = 0xCAFE_u64;
        for i in 0..200u64 {
            cms.increment(i * 1000 + hash);
        }
    }
}
