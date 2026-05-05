// crates/axcache-engine/src/bin/tester.rs
//
// Stress tester AxCache menggunakan RESP2 protocol (Redis-compatible).
// Kompatibel dengan redis-rs client library.

use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 20_000;
const VALUE_SIZE: usize = 64; // 64 bytes per value (realistic cache entry)

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

fn worker(thread_id: usize) -> TestResult {
    let mut stream =
        TcpStream::connect("127.0.0.1:6379").expect("Gagal connect ke AxCache di 127.0.0.1:6379");
    stream.set_nodelay(true).unwrap();

    let value = vec![b'A' + (thread_id as u8 % 26); VALUE_SIZE];
    let start = Instant::now();

    // ── SET benchmark ───────────────────────────────────────────────
    for i in 0..OPS_PER_THREAD {
        let key = format!("bench:{}:{}", thread_id, i);
        let cmd = resp_command(&[b"SET", key.as_bytes(), &value]);
        let msg = format!("Gagal mengirim command SET untuk key {}", key);
        stream.write_all(&cmd).expect(&msg);
        read_response(&mut stream); // consume +OK
    }

    let set_elapsed = start.elapsed();

    // ── GET benchmark ───────────────────────────────────────────────
    let get_start = Instant::now();
    let mut hits = 0usize;

    for i in 0..OPS_PER_THREAD {
        let key = format!("bench:{}:{}", thread_id, i);
        let cmd = resp_command(&[b"GET", key.as_bytes()]);
        let msg = format!("Gagal mengirim command GET untuk key {}", key);
        stream.write_all(&cmd).expect(&msg);
        let resp = read_response(&mut stream);
        if !resp.is_empty() && resp != "(nil)" {
            hits += 1;
        }
    }

    let get_elapsed = get_start.elapsed();

    let set_qps = OPS_PER_THREAD as f64 / set_elapsed.as_secs_f64();
    let get_qps = OPS_PER_THREAD as f64 / get_elapsed.as_secs_f64();

    println!(
        "[Thread {:2}] SET {:>6} ops/s ({:.6}ms) | GET {:>6} ops/s ({:.6}ms) | hits={}/{}",
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

fn main() {
    println!("AxCache Stress Tester (RESP2)");
    println!(
        "Threads: {} | Ops/thread: {} | Value: {} bytes",
        THREADS, OPS_PER_THREAD, VALUE_SIZE
    );
    println!("Pastikan AxCache sudah berjalan di 127.0.0.1:6379\n");

    let overall_start = Instant::now();
    let handles: Vec<_> = (0..THREADS)
        .map(|t| thread::spawn(move || worker(t)))
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
}
