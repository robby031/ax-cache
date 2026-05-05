// crates/axcache-evict/src/sieve.rs
//
// SIEVE: Simple and Efficient Eviction (USENIX FAST '24)
//
// Mekanisme:
//   - Satu "hand" pointer berputar melingkar
//   - Setiap entry punya bit "visited"
//   - Hand bergerak mencari entry dengan visited=false → eviksi
//   - Entry visited=true → reset ke false, hand lanjut
//
// Kompleksitas: insert O(1), read O(1), evict O(1) amortized.

use std::collections::HashMap;
use std::hash::Hash;

pub struct Sieve<K> {
    // Ring buffer: None = slot kosong / sudah dieviksi
    entries: Vec<Option<K>>,
    // O(1) lookup: key → indeks di entries
    index: HashMap<K, usize>,
    // O(1) visited bit per key
    visited: HashMap<K, bool>,
    // Free slot list untuk O(1) insertion
    free_list: Vec<usize>,
    // Posisi hand saat ini
    hand: usize,
    count: usize,
    capacity: usize,
}

impl<K: Hash + Eq + Clone> Sieve<K> {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let free_list: Vec<usize> = (0..cap).collect();
        Self {
            entries: vec![None; cap],
            index: HashMap::with_capacity(cap),
            visited: HashMap::with_capacity(cap),
            free_list,
            hand: 0,
            count: 0,
            capacity: cap,
        }
    }

    /// Tandai key sebagai diakses — O(1).
    #[inline]
    pub fn read(&mut self, key: &K) {
        if let Some(v) = self.visited.get_mut(key) {
            *v = true;
        }
    }

    /// Insert key baru; kembalikan key yang harus dihapus dari DashTable jika penuh.
    pub fn insert(&mut self, key: K) -> Option<K> {
        if self.count >= self.capacity {
            let evicted = self.evict_one()?;
            return Some(evicted);
        }
        self.place(key);
        None
    }

    /// Hapus key (user-initiated delete).
    pub fn remove(&mut self, key: &K) {
        if let Some(idx) = self.index.remove(key) {
            self.entries[idx] = None;
            self.visited.remove(key);
            self.free_list.push(idx);
            self.count -= 1;
        }
    }

    /// Reset semua state (FLUSHALL).
    pub fn clear(&mut self) {
        for e in &mut self.entries {
            *e = None;
        }
        self.index.clear();
        self.visited.clear();
        self.free_list = (0..self.capacity).collect();
        self.hand = 0;
        self.count = 0;
    }

    // --- internal ---

    fn place(&mut self, key: K) {
        let slot = self.free_list.pop().expect("free_list empty but count < capacity");
        self.entries[slot] = Some(key.clone());
        self.index.insert(key.clone(), slot);
        self.visited.insert(key, false);
        self.count += 1;
    }

    /// Jalankan hand sampai menemukan entry dengan visited=false → eviksi.
    fn evict_one(&mut self) -> Option<K> {
        let n = self.capacity;
        for _ in 0..n * 2 {
            let pos = self.hand % n;
            self.hand = (self.hand + 1) % n;

            if let Some(key) = &self.entries[pos].clone() {
                let v = self.visited.get(key).copied().unwrap_or(false);
                if v {
                    // Masih hangat → reset bit, lanjut
                    self.visited.insert(key.clone(), false);
                } else {
                    // Dingin → eviksi!
                    let evicted = key.clone();
                    self.entries[pos] = None;
                    self.index.remove(&evicted);
                    self.visited.remove(&evicted);
                    self.free_list.push(pos);
                    self.count -= 1;
                    return Some(evicted);
                }
            }
        }
        None // Semua entry panas (sangat jarang terjadi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_eviction() {
        let mut s: Sieve<u32> = Sieve::new(3);
        assert!(s.insert(1).is_none());
        assert!(s.insert(2).is_none());
        assert!(s.insert(3).is_none());
        // Kapasitas 3, insert ke-4 → eviksi
        let ev = s.insert(4);
        assert!(ev.is_some(), "harus ada eviksi");
    }

    #[test]
    fn test_visited_prevents_immediate_eviction() {
        let mut s: Sieve<u32> = Sieve::new(2);
        s.insert(1);
        s.insert(2);
        // Tandai 1 dan 2 sebagai visited
        s.read(&1);
        s.read(&2);
        // Insert 3: hand harus melewati 1 dan 2 (reset bit), akhirnya eviksi salah satu
        let ev = s.insert(3);
        // Setelah reset, hand kembali ke yang tidak visited
        assert!(ev.is_some());
    }

    #[test]
    fn test_remove() {
        let mut s: Sieve<u32> = Sieve::new(3);
        s.insert(10);
        s.insert(20);
        s.remove(&10);
        assert_eq!(s.count, 1);
        assert!(!s.index.contains_key(&10));
    }
}
