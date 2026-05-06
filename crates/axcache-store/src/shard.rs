// crates/axcache-store/src/shard.rs
//
// Shard: private storage eksklusif untuk satu CPU core (no locking, no sharing).
//
// Fitur production:
//   • TTL (Time-To-Live): lazy expiration saat get()
//   • delete(): hapus key secara eksplisit
//   • flush(): FLUSHALL — hapus semua data
//   • size(): jumlah entry aktif (tidak termasuk expired)
//   • S3-FIFO eviction O(1)

use crate::dashtable::DashTable;
use axcache_evict::s3_fifo::S3Fifo;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Shard privat eksklusif untuk satu core.
/// Menggunakan Rc<RefCell> di layer atas (worker) sehingga tidak ada overhead Mutex.
pub struct Shard {
    pub core_id: usize,
    // Slab allocator for key/value storage
    slab: axcache_alloc::slab::Slab<(String, Vec<u8>)>,
    // Use slab indices for all internal structures
    table: DashTable<usize, ()>,
    eviction: S3Fifo<usize>,
    /// TTL tracking: slab index → waktu kadaluarsa
    expiry: HashMap<usize, Instant>,
}

impl Shard {
    /// Buat shard baru. `capacity` = estimasi jumlah key maksimum.
    pub fn new(core_id: usize, capacity: usize) -> Self {
        // DashTable butuh grup; 1 grup = 16 slot → capacity/16 grup
        let groups = (capacity / 16).max(4);
        Self {
            core_id,
            slab: axcache_alloc::slab::Slab::new(capacity),
            table: DashTable::new(groups),
            eviction: S3Fifo::new(capacity),
            expiry: HashMap::with_capacity(capacity / 4),
        }
    }

    // =========================================================================
    // GET
    // =========================================================================

    /// Ambil value; lazy expiration jika TTL sudah lewat.
    #[inline]
    pub fn get(&mut self, key: &str) -> Option<&[u8]> {
        // Find the slab index for this key
        let idx = self.find_index(key)?;
        if self.is_expired_idx(idx) {
            self.delete_idx(idx);
            return None;
        }
        self.eviction.read(&idx);
        // Get value from slab
        unsafe {
            let ptr = self.slab.get_slot_ptr(idx) as *const (String, Vec<u8>);
            ptr.as_ref().map(|kv| kv.1.as_slice())
        }
    }

    // =========================================================================
    // SET / SET WITH TTL
    // =========================================================================

    /// Simpan key-value tanpa TTL.
    pub fn set(&mut self, key: String, value: Vec<u8>) {
        let t0 = std::time::Instant::now();
        // Slab allocation
        let idx = self
            .slab
            .allocate((key, value))
            .expect("Slab full")
            .as_ptr() as usize;
        let t1 = std::time::Instant::now();
        // Expiry removal
        self.expiry.remove(&idx);
        let t2 = std::time::Instant::now();
        // S3-FIFO + DashTable
        self.do_insert_idx_instrumented(idx, t0, t1, t2);
    }

    /// Simpan key-value dengan TTL (detik).
    pub fn set_ex(&mut self, key: String, value: Vec<u8>, ttl_secs: u64) {
        let t0 = std::time::Instant::now();
        let idx = self
            .slab
            .allocate((key, value))
            .expect("Slab full")
            .as_ptr() as usize;
        let t1 = std::time::Instant::now();
        let exp = Instant::now() + Duration::from_secs(ttl_secs);
        self.expiry.insert(idx, exp);
        let t2 = std::time::Instant::now();
        self.do_insert_idx_instrumented(idx, t0, t1, t2);
    }

    // =========================================================================
    // DELETE
    // =========================================================================

    /// Hapus satu key. Mengembalikan true jika key ada dan dihapus.
    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(idx) = self.find_index(key) {
            self.delete_idx(idx)
        } else {
            false
        }
    }

    /// Hapus berdasarkan slab index.
    fn delete_idx(&mut self, idx: usize) -> bool {
        self.expiry.remove(&idx);
        self.eviction.remove(&idx);
        self.table.remove(&idx)
    }

    // =========================================================================
    // EXISTS / TTL
    // =========================================================================

    /// Cek apakah key ada dan belum expired.
    pub fn exists(&mut self, key: &str) -> bool {
        if let Some(idx) = self.find_index(key) {
            if self.is_expired_idx(idx) {
                self.delete_idx(idx);
                return false;
            }
            self.table.get(&idx).is_some()
        } else {
            false
        }
    }

    /// Sisa waktu hidup dalam detik.
    /// Mengembalikan:
    ///   Some(n)  - n detik tersisa
    ///   Some(-1) - key ada tapi tidak punya TTL
    ///   None     - key tidak ada
    pub fn ttl(&mut self, key: &str) -> Option<i64> {
        let idx = self.find_index(key)?;
        if self.is_expired_idx(idx) {
            self.delete_idx(idx);
            return None;
        }
        match self.expiry.get(&idx) {
            Some(&exp) => {
                let now = Instant::now();
                if now >= exp {
                    self.delete_idx(idx);
                    None
                } else {
                    Some((exp - now).as_secs() as i64)
                }
            }
            None => Some(-1), // key ada, tidak ada TTL
        }
    }

    /// Set TTL pada key yang sudah ada. Mengembalikan false jika key tidak ada.
    pub fn expire(&mut self, key: &str, secs: u64) -> bool {
        if let Some(idx) = self.find_index(key) {
            if self.is_expired_idx(idx) {
                self.delete_idx(idx);
                return false;
            }
            let exp = Instant::now() + Duration::from_secs(secs);
            self.expiry.insert(idx, exp);
            true
        } else {
            false
        }
    }

    // =========================================================================
    // FLUSH / SIZE
    // =========================================================================

    /// Hapus semua data (FLUSHALL).
    pub fn flush(&mut self) {
        self.table.clear();
        self.eviction.clear();
        self.expiry.clear();
    }

    /// Jumlah entry aktif (tidak termasuk expired — tidak ada eager scan).
    #[inline]
    pub fn size(&self) -> usize {
        self.table.len()
    }

    // =========================================================================
    // Internal
    // =========================================================================

    /// Cek apakah key sudah expired (tanpa menghapusnya).
    #[inline]
    fn is_expired_idx(&self, idx: usize) -> bool {
        self.expiry
            .get(&idx)
            .map(|&exp| Instant::now() >= exp)
            .unwrap_or(false)
    }

    /// Insert ke DashTable dengan S3-FIFO eviction.
    fn do_insert_idx_instrumented(&mut self, idx: usize, t0: Instant, t1: Instant, t2: Instant) {
        let t3 = std::time::Instant::now();
        // S3-FIFO: insert index, dapatkan daftar index yang harus dieviksi
        let evicted = self.eviction.insert(idx);
        let t4 = std::time::Instant::now();
        for eidx in &evicted {
            self.expiry.remove(eidx);
            self.table.remove(eidx);
            // Optionally: self.slab.deallocate(...)
        }
        let t5 = std::time::Instant::now();
        // Insert index into DashTable
        self.table.insert(idx, ());
        let t6 = std::time::Instant::now();
        // Print timings (us)
        println!(
            "[INSTRUMENT] slab={}us expiry={}us s3fifo={}us evict={}us dashtable={}us",
            (t1 - t0).as_micros(),
            (t2 - t1).as_micros(),
            (t4 - t3).as_micros(),
            (t5 - t4).as_micros(),
            (t6 - t5).as_micros()
        );
    }

    /// Find the slab index for a given key
    fn find_index(&self, key: &str) -> Option<usize> {
        // Linear scan for now; can be optimized with a secondary map if needed
        for idx in 0..self.slab.capacity() {
            unsafe {
                let ptr = self.slab.get_slot_ptr(idx) as *const (String, Vec<u8>);
                if let Some(kv) = ptr.as_ref() {
                    if kv.0 == key {
                        return Some(idx);
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let mut s = Shard::new(0, 64);
        s.set("foo".to_string(), b"bar".to_vec());
        assert_eq!(s.get("foo"), Some(b"bar".as_ref()));
        assert_eq!(s.get("missing"), None);
    }

    #[test]
    fn test_delete() {
        let mut s = Shard::new(0, 64);
        s.set("k".to_string(), b"v".to_vec());
        assert!(s.delete("k"));
        assert!(!s.delete("k")); // sudah terhapus
        assert_eq!(s.get("k"), None);
    }

    #[test]
    fn test_ttl_expiration() {
        let mut s = Shard::new(0, 64);
        s.set_ex("temp".to_string(), b"data".to_vec(), 0); // TTL 0 detik → langsung expired
        // Paksa expired dengan memanipulasi expiry map
        s.expiry.insert(0, Instant::now() - Duration::from_secs(1)); // Dummy index for test
        assert_eq!(s.get("temp"), None, "harus expired");
    }

    #[test]
    fn test_ttl_not_expired() {
        let mut s = Shard::new(0, 64);
        s.set_ex("alive".to_string(), b"data".to_vec(), 3600);
        assert_eq!(s.get("alive"), Some(b"data".as_ref()));
        assert!(s.ttl("alive").unwrap() > 0);
    }

    #[test]
    fn test_exists() {
        let mut s = Shard::new(0, 64);
        s.set("x".to_string(), b"1".to_vec());
        assert!(s.exists("x"));
        assert!(!s.exists("y"));
    }

    #[test]
    fn test_flush() {
        let mut s = Shard::new(0, 64);
        for i in 0..10u8 {
            s.set(format!("k{}", i), vec![i]);
        }
        s.flush();
        assert_eq!(s.size(), 0);
        assert_eq!(s.get("k0"), None);
    }

    #[test]
    fn test_size() {
        let mut s = Shard::new(0, 64);
        assert_eq!(s.size(), 0);
        s.set("a".to_string(), b"1".to_vec());
        s.set("b".to_string(), b"2".to_vec());
        assert_eq!(s.size(), 2);
        s.delete("a");
        assert_eq!(s.size(), 1);
    }
}
