//! Benchmarks the spawner by spawning 1,000,000 futures and driving
//! them to completion. `harness = false`: run manually, e.g.
//! `cargo bench --features use-tokio`.
//!
//! Each mode runs a fixed number of iterations (default 32, override
//! with the `BENCH_ITERATIONS` environment variable) and reports
//! min / median / mean / max for both the spawn phase and the whole
//! run.
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
#[cfg(all(
    not(feature = "use-tokio"),
    any(feature = "use-smol", feature = "use-compio")
))]
use std::hint::black_box;
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
use std::time::{Duration, Instant};

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
const INPUT_SIZE: usize = 1000000;
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
const MAX_DELAY: usize = 1 << 8;

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
const DEFAULT_ITERATIONS: usize = 32;

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn env_iterations() -> usize {
    std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_ITERATIONS)
}

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn median(mut vals: Vec<Duration>) -> Duration {
    vals.sort();
    vals[vals.len() / 2]
}

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn summary_row(label: &str, vals: &[Duration]) -> String {
    let min = vals.iter().min().unwrap();
    let max = vals.iter().max().unwrap();
    let med = median(vals.to_vec());
    let mean: Duration = vals.iter().copied().sum::<Duration>() / (vals.len() as u32).max(1);
    format!("  - {label:<12} min={min:?} median={med:?} mean={mean:?} max={max:?}")
}

#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn print_summary(runs: usize, spawn: &[Duration], total: &[Duration]) {
    eprintln!("{runs} runs:");
    eprintln!("{}", summary_row("spawn", spawn));
    eprintln!("{}", summary_row("total", total));
}

// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
/// Spawn `range.len()` delayed futures onto the scope; returns the
/// spawn phase duration (no per-run printing: it would be too noisy
/// across the many iterations).
fn spawn_inputs<Sp: Spawner<usize> + Blocker>(
    s: &mut Scope<usize, Sp>,
    range: std::ops::Range<usize>,
) -> Duration {
    let start = Instant::now();
    for i in range {
        let delay = i & MAX_DELAY;
        s.spawn(async move {
            let _ = Delay::new(std::time::Duration::from_millis((MAX_DELAY - delay) as u64)).await;
            i
        });
    }
    start.elapsed()
}

#[cfg(feature = "use-tokio")]
mod tokio_benches {
    use super::*;
    use async_scoped::TokioScope;

    const WORKERS: usize = 4;

    /// One multi-threaded runtime with a shared work-stealing pool.
    pub fn work_steal(runs: usize) {
        eprintln!("--- tokio work-stealing on a shared pool, {WORKERS} workers ---");
        let mut spawn_phases = Vec::with_capacity(runs);
        let mut totals = Vec::with_capacity(runs);
        for _ in 0..runs {
            let start = Instant::now();
            let spawn = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(WORKERS)
                .build()
                .unwrap()
                .block_on(async move {
                    // `scope_and_block` releases the current worker via
                    // `block_in_place` while driving the outputs stream.
                    TokioScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE)).0
                });
            spawn_phases.push(spawn);
            totals.push(start.elapsed());
        }
        print_summary(runs, &spawn_phases, &totals);
    }

    /// One *single-threaded* runtime per worker thread; each thread
    /// drives an equal slice of the input inline (the collect future
    /// is awaited within the same `block_on`, so the current-thread
    /// runtime never needs to be re-entered).
    pub fn runtime_per_thread(runs: usize) {
        eprintln!(
            "--- tokio runtime-per-thread, {WORKERS} single-threaded runtimes, \
             {INPUT_SIZE} futures total ---"
        );
        let chunk = INPUT_SIZE / WORKERS;
        let mut spawn_phases = Vec::with_capacity(runs);
        let mut totals = Vec::with_capacity(runs);
        for _ in 0..runs {
            let start = Instant::now();
            std::thread::scope(|threads| {
                let mut handles = Vec::new();
                for t in 0..WORKERS {
                    handles.push(
                        std::thread::Builder::new()
                            .name(format!("tokio-current-thread-{t}"))
                            .spawn_scoped(threads, move || {
                                let rt = tokio::runtime::Builder::new_current_thread()
                                    .enable_all()
                                    .build()
                                    .expect("failed to build current-thread runtime");
                                rt.block_on(async move {
                                    let (spawn, _) = unsafe {
                                        // Inline `scope_and_collect`: the
                                        // outputs stream is awaited inside the
                                        // same `block_on` rather than driven
                                        // through a nested blocking call.
                                        TokioScope::scope_and_collect(|s| {
                                            spawn_inputs(s, t * chunk..(t + 1) * chunk)
                                        })
                                    }
                                    .await;
                                    spawn
                                })
                            })
                            .expect("failed to spawn worker thread"),
                    );
                }
                // Spawn phases of the four threads overlap; the slowest
                // one bounds the phase.
                let longest = handles
                    .into_iter()
                    .map(|h| h.join().expect("worker thread panicked"))
                    .max()
                    .unwrap_or_default();
                spawn_phases.push(longest);
            });
            totals.push(start.elapsed());
        }
        print_summary(runs, &spawn_phases, &totals);
    }
}

/// Single opaque scope on smol or compio, mirroring the old single
/// benchmark. The two modes are tokio-specific. smol uses the global
/// executor; set `SMOL_THREADS=4` to match the tokio-worker
/// configuration (the global executor runs on a *single* background
/// thread by default).
#[cfg(all(
    not(feature = "use-tokio"),
    any(feature = "use-smol", feature = "use-compio")
))]
fn run_single(runs: usize) {
    eprintln!("--- single scope_and_block runs ---");
    let mut spawn_phases = Vec::with_capacity(runs);
    let mut totals = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        let r = {
            #[cfg(all(not(feature = "use-compio"), feature = "use-smol"))]
            {
                use async_scoped::SmolScope;
                let (spawn, r) = SmolScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE));
                spawn_phases.push(spawn);

                r
            }
            #[cfg(all(not(feature = "use-smol"), feature = "use-compio"))]
            {
                // compio runs everything on a single-threaded runtime, so
                // this measures the spawn/driver overhead rather than
                // parallelism.
                use async_scoped::CompioScope;
                let (spawn, r) = compio::runtime::Runtime::new()
                    .expect("failed to create compio runtime")
                    .block_on(async move {
                        CompioScope::scope_and_block(|s| spawn_inputs(s, 0..INPUT_SIZE))
                    });
                spawn_phases.push(spawn);

                r
            }
        };
        totals.push(start.elapsed());
        r.into_iter().for_each(|v| {
            let _ = black_box(v);
        });
    }
    print_summary(runs, &spawn_phases, &totals);
}

fn main() {
    // Modes and iterations reporting are only wired up when a runtime
    // feature is enabled.
    #[cfg(all(
        not(feature = "use-tokio"),
        not(any(feature = "use-smol", feature = "use-compio"))
    ))]
    {
        eprintln!(
            "the `spawner` benchmark requires the `use-tokio`, \
             `use-smol` or `use-compio` feature"
        );
    }

    #[cfg(feature = "use-tokio")]
    {
        let runs = env_iterations();
        // `cargo bench` passes its own flags (e.g. `--bench`) to the
        // binary; drop anything dash-prefixed, leaving the mode names.
        let args: Vec<String> = std::env::args().skip(1).collect();
        let modes: Vec<String> = args.into_iter().filter(|a| !a.starts_with('-')).collect();
        for mode in &modes {
            match mode.as_str() {
                "work-steal" => tokio_benches::work_steal(runs),
                "runtime-per-thread" => tokio_benches::runtime_per_thread(runs),
                _ => eprintln!("unknown mode: {mode}"),
            }
        }
        if modes.is_empty() {
            tokio_benches::work_steal(runs);
            tokio_benches::runtime_per_thread(runs);
        }
    }

    #[cfg(all(
        not(feature = "use-tokio"),
        any(feature = "use-smol", feature = "use-compio")
    ))]
    {
        let runs = env_iterations();
        run_single(runs);
    }
}
