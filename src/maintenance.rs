use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::shard::Shard;

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    pub sweep_interval: Duration,
    pub max_sweep_per_shard: usize,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            sweep_interval: Duration::from_millis(500),
            max_sweep_per_shard: 64,
        }
    }
}

pub(crate) struct MaintenanceHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }
}

pub(crate) fn spawn_worker<K, V, F>(
    shards: Arc<[Shard<K, V>]>,
    config: MaintenanceConfig,
    now_fn: F,
) -> MaintenanceHandle
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    F: Fn() -> u32 + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);

    let thread = thread::Builder::new()
        .name("axcache-maintenance".into())
        .spawn(move || {
            while !stop_clone.load(Ordering::Relaxed) {
                thread::sleep(config.sweep_interval);
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let now = now_fn();
                for shard in shards.iter() {
                    shard.sweep_expired(now, config.max_sweep_per_shard);
                }
            }
        })
        .expect("failed to spawn maintenance thread");

    MaintenanceHandle {
        stop,
        thread: Some(thread),
    }
}
