// crates/axcache-engine/src/bin/tester.rs
//
// Stress tester AxCache menggunakan RESP2 protocol (Redis-compatible).
// Kompatibel dengan redis-rs client library.

use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 5_000; // 8 x 5_000 = 40_000 ops per phase
const VALUE_SIZE: usize = 32; // 32 bytes per value for more stress

/// Serialize satu RESP2 command array.
fn resp_command(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Baca satu baris RESP response dari stream.
fn read_line(stream: &mut TcpStream) -> String {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        if byte[0] != b'\r' {
            line.push(byte[0]);
        }
    }
    String::from_utf8_lossy(&line).to_string()
}

/// Baca satu RESP response dari stream.
fn read_response(stream: &mut TcpStream) -> String {
    let line = read_line(stream);
    if line.is_empty() {
        return String::new();
    }
    match line.as_bytes()[0] {
        b'+' | b'-' | b':' => line[1..].to_string(),
        b'$' => {
            let len: i64 = line[1..].parse().unwrap_or(-1);
            if len < 0 {
                return "(nil)".to_string();
            }
            let mut data = vec![0u8; len as usize + 2]; // +2 for \r\n
            stream.read_exact(&mut data).unwrap();
            String::from_utf8_lossy(&data[..len as usize]).to_string()
        }
        _ => line,
    }
}

fn worker(
    thread_id: usize,
    set_latencies: Arc<Mutex<Vec<f64>>>,
    get_latencies: Arc<Mutex<Vec<f64>>>,
) -> TestResult {
    let mut stream =
        TcpStream::connect("127.0.0.1:6379").expect("Gagal connect ke AxCache di 127.0.0.1:6379");
    stream.set_nodelay(true).unwrap();

    let mut rng = StdRng::seed_from_u64(thread_id as u64 * 12345);
    let mut value = vec![0u8; VALUE_SIZE];
    let mut set_lat = Vec::with_capacity(OPS_PER_THREAD);
    let mut get_lat = Vec::with_capacity(OPS_PER_THREAD);

    let start = Instant::now();
    // ── SET benchmark ───────────────────────────────────────────────
    for i in 0..OPS_PER_THREAD {
        rng.fill(&mut value[..]);
        let key = format!("bench:{}:{}", thread_id, i);
        let cmd = resp_command(&[b"SET", key.as_bytes(), &value]);
        let msg = format!("Gagal mengirim command SET untuk key {}", key);
        let op_start = Instant::now();
        stream.write_all(&cmd).expect(&msg);
        read_response(&mut stream); // consume +OK
        let op_lat = op_start.elapsed().as_secs_f64() * 1_000_000.0; // us
        set_lat.push(op_lat);
    }
    let set_elapsed = start.elapsed();

    // ── GET benchmark ───────────────────────────────────────────────
    let get_start = Instant::now();
    let mut hits = 0usize;
    for i in 0..OPS_PER_THREAD {
        let key = format!("bench:{}:{}", thread_id, i);
        let cmd = resp_command(&[b"GET", key.as_bytes()]);
        let msg = format!("Gagal mengirim command GET untuk key {}", key);
        let op_start = Instant::now();
        stream.write_all(&cmd).expect(&msg);
        let resp = read_response(&mut stream);
        let op_lat = op_start.elapsed().as_secs_f64() * 1_000_000.0; // us
        get_lat.push(op_lat);
        if !resp.is_empty() && resp != "(nil)" {
            hits += 1;
        }
    }
    let get_elapsed = get_start.elapsed();

    set_latencies.lock().unwrap().extend(set_lat);
    get_latencies.lock().unwrap().extend(get_lat);

    let set_qps = OPS_PER_THREAD as f64 / set_elapsed.as_secs_f64();
    let get_qps = OPS_PER_THREAD as f64 / get_elapsed.as_secs_f64();

    println!(
        "[Thread {:2}] SET {:>8} ops/s ({:.2} ms) | GET {:>8} ops/s ({:.2} ms) | hits={}/{}",
        thread_id,
        set_qps as u64,
        set_elapsed.as_secs_f64() * 1000.0,
        get_qps as u64,
        get_elapsed.as_secs_f64() * 1000.0,
        hits,
        OPS_PER_THREAD
    );

    TestResult {
        thread: thread_id as u32,
        set_ops: set_qps as u32,
        set_time_ms: set_elapsed.as_secs_f64() * 1000.0,
        get_ops: get_qps as u32,
        get_time_ms: get_elapsed.as_secs_f64() * 1000.0,
        hits: hits as u32,
        total: OPS_PER_THREAD as u32,
    }
}

#[derive(Debug)]
struct TestResult {
    thread: u32,
    set_ops: u32,
    set_time_ms: f64,
    get_ops: u32,
    get_time_ms: f64,
    hits: u32,
    total: u32,
}

fn write_csv(path: &str, results: &[TestResult]) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    let mut wtr = BufWriter::new(file);

    // Header
    wtr.write_all(b"thread,set_ops,set_time_ms,get_ops,get_time_ms,hits,total\n")?;

    for result in results {
        let csv_line = format!(
            "{},{},{},{},{},{},{}\n",
            result.thread,
            result.set_ops,
            result.set_time_ms,
            result.get_ops,
            result.get_time_ms,
            result.hits,
            result.total
        );
        wtr.write_all(csv_line.as_bytes())?;
    }

    wtr.flush()?;
    Ok(())
}

fn percentile(data: &[f64], pct: f64) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx]
}

fn main() {
    println!("AxCache Stress Tester (RESP2)");
    println!(
        "Threads: {} | Ops/thread: {} | Value: {} bytes",
        THREADS, OPS_PER_THREAD, VALUE_SIZE
    );
    println!("Pastikan AxCache sudah berjalan di 127.0.0.1:6379\n");

    let overall_start = Instant::now();
    let set_latencies = Arc::new(Mutex::new(Vec::with_capacity(THREADS * OPS_PER_THREAD)));
    let get_latencies = Arc::new(Mutex::new(Vec::with_capacity(THREADS * OPS_PER_THREAD)));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let set_lat = Arc::clone(&set_latencies);
            let get_lat = Arc::clone(&get_latencies);
            thread::spawn(move || worker(t, set_lat, get_lat))
        })
        .collect();

    let mut results = Vec::with_capacity(THREADS);
    for h in handles {
        let res = h.join().unwrap();
        results.push(res);
    }

    write_csv("benchmark_results.csv", &results).expect("Gagal menulis CSV");

    let total_secs = overall_start.elapsed().as_secs_f64();
    let total_ops = THREADS * OPS_PER_THREAD * 2; // SET + GET
    println!(
        "\nTotal: {} ops dalam {:.2}s = {:.0} ops/s",
        total_ops,
        total_secs,
        total_ops as f64 / total_secs
    );

    // Latency stats
    let set_lat = set_latencies.lock().unwrap();
    let get_lat = get_latencies.lock().unwrap();
    println!(
        "\nSET Latency (us): min={:.1} p50={:.1} p99={:.1} max={:.1}",
        set_lat.iter().cloned().fold(f64::INFINITY, f64::min),
        percentile(&set_lat, 50.0),
        percentile(&set_lat, 99.0),
        set_lat.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
    println!(
        "GET Latency (us): min={:.1} p50={:.1} p99={:.1} max={:.1}",
        get_lat.iter().cloned().fold(f64::INFINITY, f64::min),
        percentile(&get_lat, 50.0),
        percentile(&get_lat, 99.0),
        get_lat.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
}
