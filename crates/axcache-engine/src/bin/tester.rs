use std::io::Write;
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

use axcache_engine::protocol::{ClientCommand, serialize_command};

const THREADS: usize = 8;
const OPS_PER_THREAD: usize = 200_000;
const VALUE_SIZE: usize = 1024 * 1024; // 1MB per entry

fn worker(thread_id: usize) {
    let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();

    let value = vec![thread_id as u8; VALUE_SIZE];

    let start = Instant::now();

    for i in 0..OPS_PER_THREAD {
        let key = format!("key:{}:{}", thread_id, i);

        let cmd = ClientCommand::Set {
            key,
            value: value.clone(), // bisa optimize nanti
        };

        let bytes = serialize_command(&cmd).unwrap();
        stream.write_all(&bytes).unwrap();
    }

    let elapsed = start.elapsed();
    println!("Thread {} selesai dalam {:?}", thread_id, elapsed);
}

fn main() {
    println!("🔥 Stress test AxCache dimulai");

    let mut handles = vec![];

    for t in 0..THREADS {
        handles.push(thread::spawn(move || worker(t)));
    }

    for h in handles {
        h.join().unwrap();
    }

    println!("✅ Selesai");
}
