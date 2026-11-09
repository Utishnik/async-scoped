// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol"))]
use async_scoped::{
    spawner::{Blocker, Spawner},
    Scope,
};
#[cfg(any(feature = "use-tokio", feature = "use-smol"))]
use futures_timer::Delay;
use std::hint::black_box;

// Only referenced from the runtime-gated branches of `main`.
#[cfg(any(feature = "use-tokio", feature = "use-smol"))]
fn spawn_and_collect<Sp: Spawner<usize> + Blocker>(s: &mut Scope<usize, Sp>) {
    const INPUT_SIZE: usize = 1000000;
    const MAX_DELAY: usize = 1 << 8;
    for i in 0..INPUT_SIZE {
        let delay = i & MAX_DELAY;
        s.spawn(async move {
            let _ = Delay::new(std::time::Duration::from_millis((MAX_DELAY - delay) as u64)).await;
            i
        });
    }
}

fn main() {
    let r = {
        #[cfg(all(not(feature = "use-tokio"), feature = "use-smol"))]
        {
            // Smol's global executor spawns onto a worker pool sized by
            // the number of physical cores; set SMOL_THREADS to match
            // the Tokio benchmark configuration.
            use async_scoped::SmolScope;
            let (_, r) = SmolScope::scope_and_block(spawn_and_collect);

            r
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
        #[cfg(not(any(feature = "use-tokio", feature = "use-smol")))]
        {
            eprintln!(
                "the `spawner` benchmark requires the `use-tokio` or \
                 `use-smol` feature"
            );
            Vec::<usize>::new()
        }
    };

    r.into_iter().for_each(|v| {
        let _ = black_box(v);
    })
}
