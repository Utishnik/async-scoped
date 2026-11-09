//! Benchmarks the spawner by spawning 1,000,000 futures and driving
//! them to completion. `harness = false`: run manually, e.g.
//! `cargo bench --features use-tokio`.
//!
//! Under `use-tokio` two modes are available (selectable via command
//! line arguments; with no arguments both run):
//!
//! * `work-steal`: one multi-threaded tokio runtime with a shared
//!   work-stealing pool (`worker_threads(4)`).
//! * `runtime-per-thread`: four separate *single-threaded*
//!   (`new_current_thread`) runtimes, one per worker thread; each
//!   drives an equal slice of the input inline via
//!   `scope_and_collect` (no nested `block_on`: a current-thread
//!   runtime cannot be re-entered that way).
//!
//! Without `use-tokio`, the `default` mode runs the single-scope
//! benchmark on the smol or compio branches below.

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
use async_scoped::{
    spawner::{Blocker, Spawner},
    Scope,
};
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
use futures_timer::Delay;
use std::hint::black_box;
use std::time::Instant;

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
const INPUT_SIZE: usize = 1000000;
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
const MAX_DELAY: usize = 1 << 8;

// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn spawn_inputs<Sp: Spawner<usize> + Blocker>(
    s: &mut Scope<usize, Sp>,
    range: std::ops::Range<usize>,
) {
    let start = Instant::now();
    let countdown = range.len();
    for i in range {
        let delay = i & MAX_DELAY;
        s.spawn(async move {
            let _ = Delay::new(std::time::Duration::from_millis((MAX_DELAY - delay) as u64)).await;
            i
        });
    }
    eprintln!(
        "spawned {countdown} futures (spawn phase took {:?})",
        start.elapsed()
    );
}

#[cfg(feature = "use-tokio")]
mod tokio_benches {
    use super::*;
    use async_scoped::TokioScope;

    const WORKERS: usize = 4;

    /// One multi-threaded runtime with a shared work-stealing pool.
    pub fn work_steal() {
        eprintln!("--- tokio work-stealing on a shared pool, {WORKERS} workers ---");
        let start = Instant::now();
        let outputs = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .build()
            .unwrap()
            .block_on(async move {
                // `scope_and_block` releases the current worker via
                // `block_in_place` while driving the outputs stream.
                let (_, outputs) = TokioScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE));
                outputs
            });
        eprintln!("total: {:?}", start.elapsed());
        for v in outputs {
            let _ = black_box(v);
        }
    }

    /// One *single-threaded* runtime per worker thread; each thread
    /// drives an equal slice of the input inline (the collect future
    /// is awaited within the same `block_on`, so the current-thread
    /// runtime never needs to be re-entered).
    pub fn runtime_per_thread() {
        eprintln!(
            "--- tokio runtime-per-thread, {WORKERS} single-threaded runtimes, \
             {INPUT_SIZE} futures total ---"
        );
        let chunk = INPUT_SIZE / WORKERS;
        let start = Instant::now();
        std::thread::scope(|threads| {
            let mut handles = Vec::new();
            for t in 0..WORKERS {
                let span_start = Instant::now();
                handles.push(
                    std::thread::Builder::new()
                        .name(format!("tokio-current-thread-{t}"))
                        .spawn_scoped(threads, move || {
                            let rt = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("failed to build current-thread runtime");
                            let outputs = rt.block_on(async move {
                                let (_, outputs) = unsafe {
                                    // Inline `scope_and_collect`: the outputs
                                    // stream is awaited inside the same
                                    // `block_on` rather than driven through a
                                    // nested blocking call.
                                    TokioScope::scope_and_collect(|s| {
                                        spawn_inputs(s, t * chunk..(t + 1) * chunk);
                                    })
                                }
                                .await;
                                outputs
                            });
                            eprintln!("  thread {t}: {:?}", span_start.elapsed());
                            outputs
                        })
                        .expect("failed to spawn worker thread"),
                );
            }
            for h in handles {
                h.join().expect("worker thread panicked");
            }
        });
        eprintln!("total: {:?}", start.elapsed());
    }
}

/// Single opaque scope on smol or compio, mirroring the old single
/// benchmark. The two modes are tokio-specific. smol uses the global
/// executor; set `SMOL_THREADS=4` to match the tokio-worker
/// configuration (the global executor runs on a *single* background
/// thread by default).
#[cfg(not(feature = "use-tokio"))]
fn run_single() {
    eprintln!("--- single scope_and_block run ---");
    let start = Instant::now();
    let r = {
        #[cfg(all(not(feature = "use-compio"), feature = "use-smol"))]
        {
            use async_scoped::SmolScope;
            let (_, r) = SmolScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE));

            r
        }
        #[cfg(all(not(feature = "use-smol"), feature = "use-compio"))]
        {
            // compio runs everything on a single-threaded runtime, so
            // this measures the spawn/driver overhead rather than
            // parallelism.
            use async_scoped::CompioScope;
            compio::runtime::Runtime::new()
                .expect("failed to create compio runtime")
                .block_on(async move {
                    let (_, r) = CompioScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE));

                    r
                })
        }
        #[cfg(not(any(feature = "use-smol", feature = "use-compio")))]
        {
            eprintln!(
                "the `spawner` benchmark requires the `use-tokio`, \
                 `use-smol` or `use-compio` feature"
            );
            Vec::<usize>::new()
        }
    };
    eprintln!("total: {:?}", start.elapsed());
    r.into_iter().for_each(|v| {
        let _ = black_box(v);
    });
}

fn main() {
    // Modes are tokio-specific.
    #[cfg(feature = "use-tokio")]
    let args: Vec<String> = std::env::args().skip(1).collect();

    #[cfg(not(feature = "use-tokio"))]
    {
        run_single();
    }

    #[cfg(feature = "use-tokio")]
    {
        // `cargo bench` passes its own flags (e.g. `--bench`) to the
        // binary; drop anything dash-prefixed, leaving the mode names.
        let modes: Vec<String> = args.into_iter().filter(|a| !a.starts_with('-')).collect();
        for mode in &modes {
            match mode.as_str() {
                "work-steal" => tokio_benches::work_steal(),
                "runtime-per-thread" => tokio_benches::runtime_per_thread(),
                _ => eprintln!("unknown mode: {mode}"),
            }
        }
        if modes.is_empty() {
            tokio_benches::work_steal();
            tokio_benches::runtime_per_thread();
        }
    }
}
