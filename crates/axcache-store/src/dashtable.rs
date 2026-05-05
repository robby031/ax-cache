// crates/axcache-store/src/dashtable.rs
//
// DashTable: SwissTable-inspired hash map dengan SIMD fingerprint scanning.
//
// Desain:
//   • Setiap "grup" = 16 slot; metadata (fingerprint) dipisah dari data (entries)
//     untuk memaksimalkan cache locality saat scanning.
//   • H1 (bit atas hash) → indeks grup awal.
//   • H2 (bit bawah hash) → fingerprint 1 byte per slot.
//   • Probing: linear di level grup (H1, H1+1, ...) dengan wrap-around.
//   • Tombstone: DELETED=0x01 memungkinkan delete tanpa rehash.
//   • Terminasi probing: berhenti saat grup SEPENUHNYA EMPTY (all_empty).
//   • Resize: jika (live + tombstone) / total_slots > 7/8 → double capacity.

use crate::simd_scan::{
    DELETED, EMPTY, all_empty, find_available, make_fingerprint, match_fingerprint,
};
use axcache_axhash::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

const GROUP_SIZE: usize = 16;

/// Metadata satu grup: 16 byte fingerprint, muat dalam satu register SIMD 128-bit.
#[derive(Clone)]
struct GroupMeta {
    fps: [u8; GROUP_SIZE],
}

impl GroupMeta {
    #[inline]
    fn new() -> Self {
        Self {
            fps: [EMPTY; GROUP_SIZE],
        }
    }
}

pub struct DashTable<K, V> {
    meta: Vec<GroupMeta>,
    entries: Vec<Option<(K, V)>>,
    hasher: RandomState,
    /// Jumlah grup - 1 (selalu 2^n - 1 untuk masking cepat)
    group_mask: usize,
    /// Entry yang hidup
    occupied: usize,
    /// Tombstone (DELETED)
    tombstones: usize,
}

impl<K: Hash + Eq, V> DashTable<K, V> {
    /// Buat tabel baru dengan minimal `capacity_groups` grup.
    /// Kapasitas aktual dibulatkan ke atas ke 2^n.
    pub fn new(capacity_groups: usize) -> Self {
        let n_groups = capacity_groups.next_power_of_two().max(4);
        Self {
            meta: vec![GroupMeta::new(); n_groups],
            entries: (0..n_groups * GROUP_SIZE).map(|_| None).collect(),
            hasher: RandomState::new(),
            group_mask: n_groups - 1,
            occupied: 0,
            tombstones: 0,
        }
    }

    // --- public API ---

    /// Cari value berdasarkan key. O(1) rata-rata dengan SIMD scan.
    pub fn get(&self, key: &K) -> Option<&V> {
        let (h1, h2) = self.split_hash(key);
        let mut g = h1;
        loop {
            let meta = &self.meta[g];
            // SIMD: cari semua slot dengan fingerprint sama
            let mut mask = match_fingerprint(&meta.fps, h2);
            while mask != 0 {
                let bit = mask.trailing_zeros() as usize;
                let idx = g * GROUP_SIZE + bit;
                if let Some((k, v)) = &self.entries[idx] {
                    if k == key {
                        return Some(v);
                    }
                }
                mask &= mask - 1;
            }
            // Terminasi: grup sepenuhnya EMPTY → key pasti tidak ada
            if all_empty(&meta.fps) {
                return None;
            }
            g = self.next_group(g);
            if g == h1 {
                return None; // full wrap — seharusnya tidak terjadi jika load factor dijaga
            }
        }
    }

    /// Insert atau overwrite key-value. Resize otomatis jika load factor tinggi.
    pub fn insert(&mut self, key: K, value: V) {
        // Resize sebelum insert jika diperlukan
        if (self.occupied + self.tombstones) * 8 >= self.total_slots() * 7 {
            self.resize();
        }

        let (h1, h2) = self.split_hash(&key);
        let mut g = h1;
        let mut first_avail: Option<usize> = None;

        loop {
            // Salin 16 byte metadata ke stack (1 instruksi SIMD, negligible cost).
            // Ini menghindari long-lived borrow dari self.meta yang bisa berkonflik
            // dengan mutasi self.entries di bawah.
            let fps: [u8; 16] = self.meta[g].fps;

            // 1. Cek duplicate — cari slot dengan fingerprint cocok, lalu bandingkan key
            let mut dup_idx: Option<usize> = None;
            let mut mask = match_fingerprint(&fps, h2);
            while mask != 0 {
                let bit = mask.trailing_zeros() as usize;
                let idx = g * GROUP_SIZE + bit;
                if let Some((k, _)) = &self.entries[idx] {
                    if k == &key {
                        dup_idx = Some(idx);
                        break;
                    }
                }
                mask &= mask - 1;
            }
            // Jika ada duplicate, overwrite dan return — tidak perlu update occupied
            if let Some(idx) = dup_idx {
                self.entries[idx] = Some((key, value));
                return;
            }

            // 2. Catat slot tersedia pertama (EMPTY atau DELETED)
            if first_avail.is_none() {
                let avail = find_available(&fps);
                if avail != 0 {
                    let bit = avail.trailing_zeros() as usize;
                    first_avail = Some(g * GROUP_SIZE + bit);
                }
            }

            // 3. Terminasi: grup sepenuhnya EMPTY → key tidak ada lebih jauh
            if all_empty(&fps) {
                break;
            }

            g = self.next_group(g);
            if g == h1 {
                break;
            }
        }

        // Insert di slot yang ditemukan
        let idx = first_avail
            .expect("DashTable: tidak ada slot tersedia (resize seharusnya sudah terjadi)");
        let g_idx = idx / GROUP_SIZE;
        let b_idx = idx % GROUP_SIZE;
        let was_deleted = self.meta[g_idx].fps[b_idx] == DELETED;

        self.meta[g_idx].fps[b_idx] = h2;
        self.entries[idx] = Some((key, value));
        self.occupied += 1;
        if was_deleted {
            self.tombstones = self.tombstones.saturating_sub(1);
        }
    }

    /// Hapus key. Meninggalkan tombstone DELETED agar probe chain tetap valid.
    /// Mengembalikan true jika key ditemukan dan dihapus.
    pub fn remove(&mut self, key: &K) -> bool {
        let (h1, h2) = self.split_hash(key);
        let mut g = h1;
        loop {
            let fps = &self.meta[g].fps;
            let mut mask = match_fingerprint(fps, h2);
            while mask != 0 {
                let bit = mask.trailing_zeros() as usize;
                let idx = g * GROUP_SIZE + bit;
                if let Some((k, _)) = &self.entries[idx] {
                    if k == key {
                        self.meta[g].fps[bit] = DELETED;
                        self.entries[idx] = None;
                        self.occupied = self.occupied.saturating_sub(1);
                        self.tombstones += 1;
                        return true;
                    }
                }
                mask &= mask - 1;
            }
            if all_empty(fps) {
                return false;
            }
            g = self.next_group(g);
            if g == h1 {
                return false;
            }
        }
    }

    /// Jumlah entry aktif.
    #[inline]
    pub fn len(&self) -> usize {
        self.occupied
    }

    /// Hapus semua entry (FLUSHALL).
    pub fn clear(&mut self) {
        for m in &mut self.meta {
            m.fps = [EMPTY; GROUP_SIZE];
        }
        for e in &mut self.entries {
            *e = None;
        }
        self.occupied = 0;
        self.tombstones = 0;
    }

    // --- internal ---

    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        let mut h = self.hasher.build_hasher();
        key.hash(&mut h);
        h.finish()
    }

    #[inline]
    fn split_hash(&self, key: &K) -> (usize, u8) {
        let hash = self.hash_key(key);
        let h1 = (hash >> 7) as usize & self.group_mask;
        let h2 = make_fingerprint(hash);
        (h1, h2)
    }

    #[inline]
    fn next_group(&self, g: usize) -> usize {
        (g + 1) & self.group_mask
    }

    #[inline]
    fn total_slots(&self) -> usize {
        self.meta.len() * GROUP_SIZE
    }

    /// Double kapasitas dan rehash semua entry hidup (tombstone dibuang).
    fn resize(&mut self) {
        let new_n_groups = (self.meta.len() * 2).max(4);
        let mut new_table: DashTable<K, V> = DashTable::new(new_n_groups);
        new_table.hasher = RandomState::new();

        for entry in &mut self.entries {
            if let Some((k, v)) = entry.take() {
                new_table.insert(k, v);
            }
        }

        self.meta = new_table.meta;
        self.entries = new_table.entries;
        self.hasher = new_table.hasher;
        self.group_mask = new_table.group_mask;
        self.occupied = new_table.occupied;
        self.tombstones = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut t: DashTable<String, u64> = DashTable::new(4);
        t.insert("hello".to_string(), 42);
        t.insert("world".to_string(), 99);
        assert_eq!(t.get(&"hello".to_string()), Some(&42));
        assert_eq!(t.get(&"world".to_string()), Some(&99));
        assert_eq!(t.get(&"missing".to_string()), None);
    }

    #[test]
    fn test_overwrite() {
        let mut t: DashTable<String, u64> = DashTable::new(4);
        t.insert("k".to_string(), 1);
        t.insert("k".to_string(), 2);
        assert_eq!(t.get(&"k".to_string()), Some(&2));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn test_remove_tombstone() {
        let mut t: DashTable<String, u64> = DashTable::new(4);
        t.insert("a".to_string(), 10);
        t.insert("b".to_string(), 20);
        assert!(t.remove(&"a".to_string()));
        assert_eq!(t.get(&"a".to_string()), None);
        assert_eq!(t.get(&"b".to_string()), Some(&20));
        assert_eq!(t.tombstones, 1);
    }

    #[test]
    fn test_large_insert_with_resize() {
        let mut t: DashTable<u32, u32> = DashTable::new(2);
        for i in 0..500u32 {
            t.insert(i, i * 2);
        }
        for i in 0..500u32 {
            assert_eq!(t.get(&i), Some(&(i * 2)), "key {} hilang setelah resize", i);
        }
    }

    #[test]
    fn test_insert_reuses_deleted_slot() {
        let mut t: DashTable<String, u32> = DashTable::new(4);
        t.insert("x".to_string(), 1);
        t.remove(&"x".to_string());
        t.insert("x".to_string(), 99);
        assert_eq!(t.get(&"x".to_string()), Some(&99));
        assert_eq!(t.tombstones, 0); // slot DELETED dipakai ulang
    }

    #[test]
    fn test_clear() {
        let mut t: DashTable<u32, u32> = DashTable::new(4);
        for i in 0..50u32 {
            t.insert(i, i);
        }
        t.clear();
        assert_eq!(t.len(), 0);
        assert_eq!(t.tombstones, 0);
        for i in 0..50u32 {
            assert_eq!(t.get(&i), None);
        }
    }
}
