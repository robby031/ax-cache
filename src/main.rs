// axcache/src/main.rs

use axcache_engine::worker::spawn_pinned_worker;

fn main() {
    println!("=============================================");
    println!("🚀 Memulai AxCache MVP (Zero-Tax Architecture)");
    println!("=============================================");

    // Deteksi jumlah core di mesin Anda
    let cores = core_affinity::get_core_ids().unwrap().len();
    println!("Mendeteksi {} CPU cores pada sistem.", cores);

    let mut handles = vec![];

    // MVP: Untuk malam ini, kita jalankan 1 Worker di Core 0 pada port 6379 (Port standar Redis)
    let port = 6379;
    let handle = spawn_pinned_worker(0, port);
    handles.push(handle);

    // Blokir main thread agar aplikasi terus berjalan
    for h in handles {
        h.join().unwrap();
    }
}
