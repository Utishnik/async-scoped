// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
use async_scoped::{
    spawner::{Blocker, Spawner},
    Scope,
};
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
use futures_timer::Delay;
use std::hint::black_box;

// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio"))]
fn spawn_and_collect<Sp: Spawner<usize> + Blocker>(s: &mut Scope<usize, Sp>) {
    const INPUT_SIZE: usize = 1000000;
    const MAX_DELAY: usize = 1 << 8;
    let start = std::time::Instant::now();
    for i in 0..INPUT_SIZE {
        let delay = i & MAX_DELAY;
        s.spawn(async move {
            let _ = Delay::new(std::time::Duration::from_millis((MAX_DELAY - delay) as u64)).await;
            i
        });
    }
    eprintln!("spawned {} futures in {:?}", INPUT_SIZE, start.elapsed());
}

fn main() {
    let start = std::time::Instant::now();
    let r = {
        #[cfg(all(not(feature = "use-tokio"), feature = "use-smol"))]
        {
            // Smol's global executor runs on a single background thread
            // by default (SMOL_THREADS unset). Set SMOL_THREADS=4 to
            // match the Tokio benchmark configuration.
            use async_scoped::SmolScope;
            let (_, r) = SmolScope::scope_and_block(spawn_and_collect);

            r
        }
        #[cfg(all(not(feature = "use-tokio"), feature = "use-compio"))]
        {
            // compio runs everything on a single-threaded runtime, so
            // this measures the spawn/driver overhead rather than
            // parallelism.
            use async_scoped::CompioScope;
            compio::runtime::Runtime::new()
                .expect("failed to create compio runtime")
                .block_on(async move {
                    let (_, r) = CompioScope::scope_and_block(spawn_and_collect);

                    r
                })
        }
        #[cfg(feature = "use-tokio")]
        {
            use async_scoped::TokioScope;
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .build()
                .unwrap()
                .block_on(async move {
                    let (_, r) = TokioScope::scope_and_block(spawn_and_collect);

                    r
                })
        }
        #[cfg(not(any(feature = "use-tokio", feature = "use-smol", feature = "use-compio")))]
        {
            eprintln!(
                "the `spawner` benchmark requires the `use-tokio`, \
                 `use-smol` or `use-compio` feature"
            );
            Vec::<usize>::new()
        }
    };
    eprintln!(
        "spawned and collected all outputs in {:?} (including the spawn phase)",
        start.elapsed()
    );

    r.into_iter().for_each(|v| {
        let _ = black_box(v);
    })
}
