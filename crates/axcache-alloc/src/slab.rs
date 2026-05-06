// crates/axcache-alloc/src/slab.rs

use std::alloc::{Layout, alloc, dealloc};
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

/// Ukuran Cache Line standar pada CPU modern (x86_64, ARM64)
const CACHE_LINE_SIZE: usize = 64;

/// Slab Allocator untuk tipe data T.
/// Mengelola satu blok memori besar dari Global Allocator (mimalloc)
/// dan membaginya ke dalam slot-slot yang memiliki cache-locality tinggi.
pub struct Slab<T> {
    /// Pointer ke awal dari blok memori besar
    memory: NonNull<u8>,
    /// Layout yang digunakan saat alokasi ke OS (digunakan lagi saat dealloc/drop)
    layout: Layout,
    /// Total kapasitas slot dalam slab ini
    capacity: usize,
    /// Indeks ke slot pertama yang kosong (Free-list head)
    free_head: Option<usize>,
    /// Penanda tipe untuk borrow checker Rust
    _marker: std::marker::PhantomData<T>,
}

// Menjamin Slab aman dikirim lintas thread jika tipe T aman
unsafe impl<T: Send> Send for Slab<T> {}
unsafe impl<T: Sync> Sync for Slab<T> {}

impl<T> Slab<T> {
    /// Menginisialisasi Slab baru dengan alokasi besar ke OS (hanya dipanggil sekali).
    /// Memaksa perataan (alignment) sejajar dengan Cache Line CPU.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Kapasitas Slab tidak boleh nol");

        // 1. Perhitungan Ukuran dan Alignment
        // Kita memastikan bahwa setiap ukuran slot minimal sebesar `usize` agar
        // muat untuk menyimpan pointer indeks (free list) saat slot sedang kosong.
        let slot_size = size_of::<T>().max(size_of::<usize>());

        // Kita juga bisa memaksa align_of::<T>() sejalan dengan CACHE_LINE_SIZE
        // jika T adalah struktur yang sangat sering diakses secara paralel.
        let alignment = align_of::<T>().max(CACHE_LINE_SIZE);

        let total_size = slot_size * capacity;

        // 2. Alokasi Menggunakan mimalloc (via global_allocator yang membypass glibc)
        let layout = Layout::from_size_align(total_size, alignment)
            .expect("Gagal mengonstruksi Layout memori untuk Slab");

        let memory = unsafe {
            let ptr = alloc(layout);
            NonNull::new(ptr).expect("OOM: Sistem kehabisan memori untuk alokasi Slab")
        };

        let mut slab = Self {
            memory,
            layout,
            capacity,
            free_head: Some(0), // Slot pertama (0) adalah head dari free-list
            _marker: std::marker::PhantomData,
        };

        // 3. Bangun Intrusive Free-List di dalam memori yang baru dialokasikan
        slab.initialize_free_list(slot_size);

        slab
    }

    /// Menghubungkan semua slot yang kosong ke dalam bentuk linked-list tersembunyi
    fn initialize_free_list(&mut self, slot_size: usize) {
        unsafe {
            let base_ptr = self.memory.as_ptr();

            for i in 0..(self.capacity - 1) {
                // Kalkulasi alamat memori untuk slot ke-i
                let slot_ptr = base_ptr.add(i * slot_size) as *mut usize;

                // Tulis indeks slot selanjutnya ke dalam ruang slot ini
                std::ptr::write(slot_ptr, i + 1);
            }

            // Slot terakhir tidak memiliki slot selanjutnya
            let last_ptr = base_ptr.add((self.capacity - 1) * slot_size) as *mut usize;
            std::ptr::write(last_ptr, usize::MAX);
        }
    }

    /// Mengambil raw pointer ke slot tertentu berdasarkan ukurannya
    #[inline(always)]
    pub fn get_slot_ptr(&self, index: usize) -> *mut u8 {
        let slot_size = size_of::<T>().max(size_of::<usize>());
        unsafe { self.memory.as_ptr().add(index * slot_size) }
    }

    /// Mengambil satu slot memori O(1) tanpa memanggil syscall malloc/OS.
    pub fn allocate(&mut self, value: T) -> Option<NonNull<T>> {
        let head_index = self.free_head?;

        unsafe {
            let slot_ptr = self.get_slot_ptr(head_index);

            // Baca indeks slot kosong berikutnya yang tersimpan di head_index
            let next_free = std::ptr::read(slot_ptr as *const usize);

            // Perbarui penunjuk free-list global dari Slab
            self.free_head = if next_free == usize::MAX {
                None // Slab penuh
            } else {
                Some(next_free)
            };

            // Tulis nilai (value) sebenarnya ke dalam slot tersebut
            let val_ptr = slot_ptr as *mut T;
            std::ptr::write(val_ptr, value);

            Some(NonNull::new_unchecked(val_ptr))
        }
    }

    /// Membebaskan slot O(1) ke dalam free-list tanpa memanggil syscall free.
    /// Wajib memberikan indeks asli agar bisa dikembalikan ke rantai list.
    pub fn deallocate(&mut self, ptr: NonNull<T>, index: usize) {
        // Hancurkan objek lama secara aman (jika objek mengimplementasikan Drop)
        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };

        // Konversi pointer objek kembali menjadi tempat penyimpanan free-list
        let slot_ptr = ptr.as_ptr() as *mut usize;

        // Slot ini sekarang menunjuk ke mantan kepala (head) antrian kosong
        unsafe { std::ptr::write(slot_ptr, self.free_head.unwrap_or(usize::MAX)) };

        // Update kepala antrian menjadi slot yang baru saja dibebaskan ini
        self.free_head = Some(index);
    }

    /// Mendapatkan sisa kapasitas yang masih tersedia
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Menghancurkan keseluruhan Slab.
/// Disinilah satu-satunya tempat `dealloc` OS dipanggil.
impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        unsafe {
            // Kembalikan bongkahan besar ke mimalloc / OS
            dealloc(self.memory.as_ptr(), self.layout);
        }
    }
}
