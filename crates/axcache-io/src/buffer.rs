// crates/axcache-io/src/buffer.rs

/// IoBuffer adalah representasi buffer statis yang kepemilikannya (ownership)
/// bisa diserahkan ke kernel via io_uring dan dikembalikan lagi ke user space.
pub struct IoBuffer {
    inner: Vec<u8>,
}

impl IoBuffer {
    /// Membuat buffer baru dengan kapasitas spesifik.
    pub fn new(capacity: usize) -> Self {
        let mut inner = Vec::with_capacity(capacity);
        // Inisialisasi panjang buffer agar siap diisi oleh kernel
        unsafe { inner.set_len(capacity) };
        Self { inner }
    }

    /// Mengambil ownership untuk diberikan ke monoio/kernel
    #[inline(always)]
    pub fn into_inner(self) -> Vec<u8> {
        self.inner
    }

    /// Menerima kembali ownership dari monoio/kernel setelah I/O selesai
    #[inline(always)]
    pub fn from_inner(inner: Vec<u8>) -> Self {
        Self { inner }
    }

    /// Mengembalikan referensi slice data yang valid (yang sudah dibaca)
    #[inline(always)]
    pub fn as_slice(&self, len: usize) -> &[u8] {
        &self.inner[..len]
    }
}
