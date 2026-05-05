// crates/axcache-evict/src/s3_fifo.rs
//
// S3-FIFO: Simple, Scalable, 3-Queue FIFO (SOSP '23)
// Semua operasi insert/read: O(1) menggunakan HashMap untuk visited bits.
//
// Tiga antrian:
//   Small (10%)  - filter "one-hit wonders" baru
//   Main  (90%)  - data populer yang terbukti diakses >1x
//   Ghost (=Main)- metadata saja dari objek yang baru dieviksi

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

pub struct S3Fifo<K> {
    small_q: VecDeque<K>,
    main_q: VecDeque<K>,
    ghost_q: VecDeque<K>,

    // O(1): visited bit per key yang aktif di small/main
    visited: HashMap<K, bool>,
    // O(1): membership tracking
    in_small: HashSet<K>,
    in_main: HashSet<K>,
    ghost_set: HashSet<K>,

    small_capacity: usize,
    main_capacity: usize,
    ghost_capacity: usize,
}

impl<K: Hash + Eq + Clone> S3Fifo<K> {
    pub fn new(capacity: usize) -> Self {
        let small_cap = ((capacity as f64 * 0.1) as usize).max(1);
        let main_cap = capacity.saturating_sub(small_cap).max(1);
        let ghost_cap = main_cap;

        Self {
            small_q: VecDeque::with_capacity(small_cap),
            main_q: VecDeque::with_capacity(main_cap),
            ghost_q: VecDeque::with_capacity(ghost_cap),
            visited: HashMap::with_capacity(capacity),
            in_small: HashSet::with_capacity(small_cap),
            in_main: HashSet::with_capacity(main_cap),
            ghost_set: HashSet::with_capacity(ghost_cap),
            small_capacity: small_cap,
            main_capacity: main_cap,
            ghost_capacity: ghost_cap,
        }
    }

    /// Tandai key sebagai diakses — O(1) HashMap lookup.
    #[inline]
    pub fn read(&mut self, key: &K) {
        if self.in_small.contains(key) || self.in_main.contains(key) {
            self.visited.insert(key.clone(), true);
        }
    }

    /// Insert key baru; kembalikan keys yang harus dihapus dari DashTable.
    pub fn insert(&mut self, key: K) -> Vec<K> {
        let mut evicted = Vec::new();

        if self.ghost_set.contains(&key) {
            // Pernah dieviksi tapi diakses lagi → promosi langsung ke Main
            self.ghost_set.remove(&key);
            while self.in_main.len() >= self.main_capacity {
                if let Some(k) = self.evict_main() {
                    evicted.push(k);
                }
            }
            self.in_main.insert(key.clone());
            self.main_q.push_back(key.clone());
            self.visited.insert(key, false);
        } else {
            // Key baru → masuk Small untuk disaring
            while self.in_small.len() >= self.small_capacity {
                self.drain_small(&mut evicted);
            }
            self.in_small.insert(key.clone());
            self.small_q.push_back(key.clone());
            self.visited.insert(key, false);
        }

        evicted
    }

    /// Hapus key dari semua tracking (user-initiated delete).
    #[inline]
    pub fn remove(&mut self, key: &K) {
        self.visited.remove(key);
        self.in_small.remove(key);
        self.in_main.remove(key);
        self.ghost_set.remove(key);
        // VecDeque entries di-skip saat eviction karena in_small/in_main sudah kosong
    }

    /// Reset semua state (FLUSHALL).
    pub fn clear(&mut self) {
        self.small_q.clear();
        self.main_q.clear();
        self.ghost_q.clear();
        self.visited.clear();
        self.in_small.clear();
        self.in_main.clear();
        self.ghost_set.clear();
    }

    // --- internal ---

    /// Kosongkan satu slot dari Small. Jika item visited → promosi ke Main.
    fn drain_small(&mut self, evicted: &mut Vec<K>) {
        while let Some(key) = self.small_q.pop_front() {
            if !self.in_small.contains(&key) {
                continue; // sudah di-remove manual
            }
            self.in_small.remove(&key);
            let visited = self.visited.remove(&key).unwrap_or(false);

            if visited {
                // Promosi ke Main — pastikan ada ruang
                while self.in_main.len() >= self.main_capacity {
                    if let Some(k) = self.evict_main() {
                        evicted.push(k);
                    }
                }
                self.in_main.insert(key.clone());
                self.main_q.push_back(key.clone());
                self.visited.insert(key, false);
            } else {
                self.add_to_ghost(key.clone());
                evicted.push(key);
            }
            return;
        }
    }

    /// Eviksi satu item dari Main. Item dengan visited=true mendapat satu putaran lagi.
    fn evict_main(&mut self) -> Option<K> {
        let limit = self.main_q.len() + 1;
        for _ in 0..limit {
            let key = self.main_q.pop_front()?;
            if !self.in_main.contains(&key) {
                continue; // sudah di-remove manual
            }
            let visited = self.visited.remove(&key).unwrap_or(false);
            self.in_main.remove(&key);

            if visited {
                // Masih aktif → kembalikan ke antrian dengan bit cleared
                self.in_main.insert(key.clone());
                self.main_q.push_back(key.clone());
                self.visited.insert(key, false);
            } else {
                return Some(key);
            }
        }
        None
    }

    fn add_to_ghost(&mut self, key: K) {
        if self.ghost_q.len() >= self.ghost_capacity {
            if let Some(old) = self.ghost_q.pop_front() {
                self.ghost_set.remove(&old);
            }
        }
        self.ghost_set.insert(key.clone());
        self.ghost_q.push_back(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_is_o1() {
        // Insert 50 item (< small_cap=100 dari kap 1000) agar tidak ada eviksi
        let mut f: S3Fifo<u64> = S3Fifo::new(1000);
        for i in 0..50u64 {
            f.insert(i);
        }
        // Semua key masih di small → visited bisa di-set via O(1) HashMap
        f.read(&0);
        f.read(&49);
        assert_eq!(*f.visited.get(&0).unwrap(), true, "key 0 harus visited");
        assert_eq!(*f.visited.get(&49).unwrap(), true, "key 49 harus visited");
        // Key yang tidak di-insert tidak boleh ada
        assert!(f.visited.get(&100u64).is_none());
    }

    #[test]
    fn test_visited_prevents_eviction() {
        // cap 2: small=1, main=1
        let mut f: S3Fifo<&str> = S3Fifo::new(2);
        f.insert("A");
        f.read(&"A");
        // B masuk → small penuh → drain A: A visited → promosi ke main, bukan dieviksi
        let ev = f.insert("B");
        assert!(!ev.contains(&"A"), "A harus dipromosikan, bukan dieviksi");
        assert!(f.in_main.contains(&"A"));
    }

    #[test]
    fn test_ghost_promotion() {
        let mut f: S3Fifo<&str> = S3Fifo::new(2);
        f.insert("A");
        let ev = f.insert("B"); // A tidak visited → eviksi ke ghost
        assert!(ev.contains(&"A"));
        assert!(f.ghost_set.contains(&"A"));

        // Re-insert A → langsung ke main (ghost hit)
        let ev2 = f.insert("A");
        assert!(!ev2.contains(&"A"));
        assert!(f.in_main.contains(&"A"));
    }
}
