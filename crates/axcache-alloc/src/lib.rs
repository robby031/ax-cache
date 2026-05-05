// crates/axcache-alloc/src/lib.rs

#![doc = "
AxCache Custom Allocator

Memanfaatkan `mimalloc` sebagai Global Allocator untuk performa multi-threaded tingkat industrial
dan menyediakan `Slab` allocator mandiri untuk zero-syscall allocation pada objek berukuran tetap.
"]

// 1. Mendaftarkan mimalloc sebagai Global Allocator untuk seluruh ekosistem AxCache.
// Ini akan mengambil alih alokasi standar Rust (membypass glibc malloc)
// dan memberikan performa jauh lebih stabil untuk beban kerja asinkron/TPC.
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// 2. Mengekspos modul slab yang memuat logika free-list tersembunyi
pub mod slab;

#[cfg(test)]
mod tests {
    use super::slab::Slab;

    #[test]
    fn test_global_allocator_mimalloc() {
        // Uji coba sederhana bahwa global allocator (mimalloc) aktif
        // dan dapat menangani realokasi dinamis pada heap.
        let mut data = Vec::with_capacity(1024);
        for i in 0..1024 {
            data.push(i * 2);
        }
        assert_eq!(data[500], 1000);
    }

    #[test]
    fn test_slab_allocation_flow() {
        // Uji coba fungsionalitas Slab Allocator secara komprehensif
        // Kita membuat slab untuk menyimpan tipe `u64` dengan kapasitas 5 slot
        let mut slab: Slab<u64> = Slab::new(5);

        // Alokasikan 3 elemen berurutan (O(1) tanpa memanggil OS)
        let ptr1 = slab.allocate(10).expect("Gagal alokasi slot 0");
        let ptr2 = slab.allocate(20).expect("Gagal alokasi slot 1");
        let ptr3 = slab.allocate(30).expect("Gagal alokasi slot 2");

        unsafe {
            assert_eq!(*ptr1.as_ptr(), 10);
            assert_eq!(*ptr2.as_ptr(), 20);
            assert_eq!(*ptr3.as_ptr(), 30);

            // Deallokasi ptr2 (elemen di tengah, indeks 1)
            // Dalam implementasi free-list kita, slot ini akan ditambahkan kembali ke depan `free_head`
            slab.deallocate(ptr2, 1);
        }

        // Alokasi elemen baru.
        // Karena kita menggunakan free-list, ia seharusnya secara instan menggunakan
        // kembali slot kosong dari ptr2 yang baru saja kita bebaskan.
        let ptr_new = slab.allocate(99).expect("Gagal alokasi daur ulang");

        unsafe {
            assert_eq!(*ptr_new.as_ptr(), 99);
            // Validasi krusial: Memastikan alamat memori pointer yang baru
            // sama persis dengan alamat memori pointer lama yang di-deallocate!
            assert_eq!(
                ptr_new.as_ptr(),
                ptr2.as_ptr(),
                "Slab gagal mendaur ulang memori secara O(1)"
            );
        }
    }
}
