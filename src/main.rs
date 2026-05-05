// axcache/src/main.rs
//
// Entry point AxCache — Thread-Per-Core production server.
//
// Setiap CPU core mendapatkan:
//   • 1 thread yang di-pin ke core tersebut
//   • 1 Monoio runtime dengan io_uring backend
//   • 1 Shard data eksklusif (tanpa sharing, tanpa locking)
//   • 1 TCP listener pada port 6379 dengan SO_REUSEPORT
//
// Kernel Linux/macOS mendistribusikan koneksi secara otomatis ke semua worker.

use axcache_engine::worker::spawn_pinned_worker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() {
    print_banner();

    let n_cores = core_affinity::get_core_ids()
        .map(|ids| ids.len())
        .unwrap_or(1);

    eprintln!("Terdeteksi {} CPU core(s).", n_cores);
    eprintln!("Port: 6379 (Redis-compatible, SO_REUSEPORT)");
    eprintln!("Tekan Ctrl+C untuk shutdown graceful.\n");

    // Shared shutdown flag: main thread set → semua worker berhenti
    let shutdown = Arc::new(AtomicBool::new(false));

    // Install Ctrl+C handler
    let shutdown_signal = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        eprintln!("\nShutdown signal diterima. Menghentikan semua worker...");
        shutdown_signal.store(true, Ordering::Relaxed);
    })
    .expect("Gagal memasang Ctrl+C handler");

    // Spawn satu worker per core, semuanya mendengar port 6379
    let port: u16 = 6379;
    let handles: Vec<_> = (0..n_cores)
        .map(|core_id| spawn_pinned_worker(core_id, port, Arc::clone(&shutdown)))
        .collect();

    eprintln!("AxCache aktif — {} worker(s) pada port {}.", n_cores, port);

    // Tunggu semua worker selesai (terjadi saat shutdown flag di-set)
    for h in handles {
        let _ = h.join();
    }

    eprintln!("AxCache berhenti dengan bersih.");
}

fn print_banner() {
    eprintln!(
        r#"
  ___            ____           _
 / _ \__  __   / ___|__ _  ___| |__   ___
| | | \ \/ /  | |   / _` |/ __| '_ \ / _ \
| |_| |>  <   | |__| (_| | (__| | | |  __/
 \___//_/\_\   \____\__,_|\___|_| |_|\___|

 Version  : 0.1.0
 Protocol : RESP2 (Redis-compatible)
 Algorithm: S3-FIFO + DashTable (SIMD)
 Runtime  : Monoio (io_uring/kqueue)
 HashFunc : AxHash 65 GiB/s
"#
    );
}
