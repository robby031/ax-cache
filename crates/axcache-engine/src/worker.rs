// crates/axcache-engine/src/worker.rs
//
// Thread-Per-Core Worker: satu thread dipinning ke satu CPU core, menjalankan
// satu Monoio runtime (io_uring), mengelola satu Shard secara eksklusif.
//
// Arsitektur:
//   Main thread (main.rs)
//     ├─ Thread 0 (Core 0) → Monoio → io_uring → Shard 0 → port 6379 [SO_REUSEPORT]
//     ├─ Thread 1 (Core 1) → Monoio → io_uring → Shard 1 → port 6379 [SO_REUSEPORT]
//     └─ Thread N ...
//
// Tidak ada locking. Tidak ada shared state antar worker.
// Komunikasi antar core hanya melalui SPSC queue (future work).

use crate::resp::{RespParser, execute};
use axcache_io::buffer::IoBuffer;
use axcache_io::net::{CoreListener, read_stream, write_stream};
use axcache_store::shard::Shard;
use core_affinity::CoreId;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

// Kapasitas awal shard per core. DashTable akan auto-resize.
// 1M item untuk production deployment standard.
const SHARD_INITIAL_CAPACITY: usize = 1_048_576; // 1M

// Buffer baca per koneksi (4KB — sweet spot untuk network payload cache)
const READ_BUFFER_SIZE: usize = 4096;

/// Satu unit worker yang berjalan di dedicated CPU core.
pub struct WorkerNode {
    pub core_id: usize,
    /// Shard eksklusif — hanya diakses dari 1 thread via Monoio task pool
    shard: Rc<RefCell<Shard>>,
    /// Signal shutdown dari main thread
    shutdown: Arc<AtomicBool>,
}

impl WorkerNode {
    pub fn new(core_id: usize, shutdown: Arc<AtomicBool>) -> Self {
        Self {
            core_id,
            shard: Rc::new(RefCell::new(Shard::new(core_id, SHARD_INITIAL_CAPACITY))),
            shutdown,
        }
    }

    /// Event loop utama: accept → spawn per-connection task.
    /// Berjalan sampai shutdown flag di-set.
    pub async fn run(self, port: u16) {
        // SO_REUSEPORT: kernel load-balance koneksi ke semua worker yang bind port sama
        let listener = match CoreListener::bind_reuseport(port) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[Core {}] Gagal bind port {}: {}", self.core_id, port, e);
                return;
            }
        };

        eprintln!(
            "[Core {}] Listening 0.0.0.0:{} (SO_REUSEPORT)",
            self.core_id, port
        );

        loop {
            // Cek shutdown sebelum setiap accept
            if self.shutdown.load(Ordering::Relaxed) {
                eprintln!("[Core {}] Shutdown, keluar dari accept loop.", self.core_id);
                break;
            }

            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let shard = Rc::clone(&self.shard);
                    let core_id = self.core_id;
                    let shutdown = Arc::clone(&self.shutdown);

                    // Spawn task dalam runtime Monoio yang sama (single-threaded)
                    monoio::spawn(async move {
                        handle_connection(stream, shard, core_id, shutdown).await;
                    });
                }
                Err(e) => {
                    eprintln!("[Core {}] Accept error: {}", self.core_id, e);
                }
            }
        }
    }
}

/// Handle satu koneksi klien — persistent connection (pipelining support).
async fn handle_connection(
    stream: monoio::net::TcpStream,
    shard: Rc<RefCell<Shard>>,
    core_id: usize,
    shutdown: Arc<AtomicBool>,
) {
    let mut active_stream = stream;
    let mut parser = RespParser::new();
    let mut response_buf: Vec<u8> = Vec::with_capacity(READ_BUFFER_SIZE);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // ── PHASE 1: Read ──────────────────────────────────────────────
        let buffer = IoBuffer::new(READ_BUFFER_SIZE);
        let (res, returned_buf, stream_back) = read_stream(active_stream, buffer).await;
        active_stream = stream_back;

        let n = match res {
            Ok(0) => break, // EOF: client closed
            Ok(n) => n,
            Err(_) => break,
        };

        // Feed bytes ke parser streaming
        parser.feed(returned_buf.as_slice(n));

        // ── PHASE 2: Parse + Execute (bisa batch banyak command sekaligus) ──
        response_buf.clear();
        loop {
            match parser.try_parse() {
                None => break, // tidak ada command lengkap lagi
                Some(args) => {
                    if args.is_empty() {
                        continue; // baris kosong, skip
                    }
                    // Eksekusi command terhadap shard lokal
                    let resp = {
                        let mut s = shard.borrow_mut();
                        execute(&args, &mut s)
                    };
                    response_buf.extend_from_slice(&resp);

                    // QUIT: kirim response lalu tutup koneksi
                    let cmd_upper: Vec<u8> =
                        args[0].iter().map(|b| b.to_ascii_uppercase()).collect();
                    if cmd_upper == b"QUIT" {
                        let (_, _, stream_back) = write_stream(active_stream, response_buf).await;
                        let _ = stream_back;
                        return;
                    }
                }
            }
        }

        // ── PHASE 3: Write batch response ──────────────────────────────
        if !response_buf.is_empty() {
            let to_send =
                std::mem::replace(&mut response_buf, Vec::with_capacity(READ_BUFFER_SIZE));
            let (res, _, stream_back) = write_stream(active_stream, to_send).await;
            active_stream = stream_back;
            if res.is_err() {
                break;
            }
        }
    }

    let _ = core_id; // digunakan di log jika perlu
}

/// Spawn satu worker thread yang di-pin ke CPU core tertentu.
///
/// # Arguments
/// * `logical_core_id` - Indeks core CPU (0..n_cores)
/// * `port` - Port TCP (default 6379, shared via SO_REUSEPORT)
/// * `shutdown` - Arc<AtomicBool> untuk graceful shutdown
pub fn spawn_pinned_worker(
    logical_core_id: usize,
    port: u16,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Pin thread ke core
        let core_ids = core_affinity::get_core_ids().expect("Gagal membaca topologi CPU");
        if logical_core_id < core_ids.len() {
            let core: CoreId = core_ids[logical_core_id];
            core_affinity::set_for_current(core);
            eprintln!(
                "[Core {}] Thread pinned ke CPU {}",
                logical_core_id, core.id
            );
        } else {
            eprintln!(
                "[Core {}] Peringatan: core ID melebihi jumlah CPU ({}), lanjut tanpa pinning.",
                logical_core_id,
                core_ids.len()
            );
        }

        // Build Monoio runtime dengan io_uring (FusionDriver = io_uring + fallback kqueue/epoll)
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .with_entries(4096) // io_uring queue depth
            .build()
            .expect("Gagal build Monoio runtime");

        let worker = WorkerNode::new(logical_core_id, shutdown);
        rt.block_on(worker.run(port));
    })
}
