//! There you can find traits that are necessary for implementing executor.
//! They are mostly unsafe, as we can't ensure the executor allows intended manipulations
//! without causing UB.
use futures::Future;

pub unsafe trait Spawner<T> {
    type FutureOutput;
    type SpawnHandle: Future<Output = Self::FutureOutput> + Send;
    fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle;
}

pub unsafe trait FuncSpawner<T> {
    type FutureOutput;
    type SpawnHandle: Future<Output = Self::FutureOutput> + Send;
    fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle;
}

pub unsafe trait Blocker {
    fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T;
}

#[cfg(feature = "use-smol")]
pub mod use_smol {
    use super::*;
    use smol::{spawn, Task};

    #[derive(Default)]
    pub struct Smol;

    unsafe impl<T: Send + 'static> Spawner<T> for Smol {
        type FutureOutput = T;
        type SpawnHandle = Task<T>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            spawn(f)
        }
    }

    unsafe impl<T: Send + 'static> FuncSpawner<T> for Smol {
        type FutureOutput = T;
        type SpawnHandle = Task<T>;

        fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // `smol` does not provide an async-std-like blocking pool
            // out of the box, so run the closure on a dedicated thread
            // and deliver the result through a one-shot channel. The
            // handle is a regular `smol` task that only waits on the
            // channel, never on the blocking work itself.
            let (tx, rx) = futures::channel::oneshot::channel();
            std::thread::spawn(move || {
                let _ = tx.send(f());
            });
            spawn(async move {
                rx.await
                    .expect("blocking function panicked or thread was dropped")
            })
        }
    }
    unsafe impl Blocker for Smol {
        fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T {
            smol::block_on(f)
        }
    }
}

#[cfg(feature = "use-tokio")]
pub mod use_tokio {
    use super::*;
    use tokio::task as tokio_task;

    #[derive(Default)]
    pub struct Tokio;

    unsafe impl<T: Send + 'static> Spawner<T> for Tokio {
        type FutureOutput = Result<T, tokio_task::JoinError>;
        type SpawnHandle = tokio_task::JoinHandle<T>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            tokio_task::spawn(f)
        }
    }

    unsafe impl<T: Send + 'static> FuncSpawner<T> for Tokio {
        type FutureOutput = Result<T, tokio_task::JoinError>;
        type SpawnHandle = tokio_task::JoinHandle<T>;

        fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            tokio_task::spawn_blocking(f)
        }
    }

    unsafe impl Blocker for Tokio {
        fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T {
            tokio_task::block_in_place(|| {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(f)
            })
        }
    }
}

#[cfg(feature = "use-compio")]
pub mod use_compio {
    use super::*;
    use compio::runtime::{spawn, spawn_blocking, Task};

    /// The output of a spawned compio task: the value itself, or
    /// `Err` carrying the panic payload if the task panicked.
    pub type TaskResult<T> =
        std::result::Result<T, std::boxed::Box<dyn std::any::Any + std::marker::Send>>;

    #[derive(Default)]
    pub struct Compio;

    unsafe impl<T: Send + 'static> Spawner<T> for Compio {
        type FutureOutput = TaskResult<T>;
        type SpawnHandle = Task<TaskResult<T>>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // compio wraps the future in `AssertUnwindSafe(f).catch_unwind()`
            // and returns a `JoinHandle<T> = Task<Result<T, Box<dyn Any +
            // Send>>>`, so the handle already yields the task's output or
            // its panic payload; no further wrapping is needed.
            spawn(f)
        }
    }

    unsafe impl<T: Send + 'static> FuncSpawner<T> for Compio {
        type FutureOutput = TaskResult<T>;
        type SpawnHandle = Task<TaskResult<T>>;

        fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // `compio` provides `spawn_blocking`, which runs the closure
            // on a dedicated thread and delivers the result back to the
            // runtime asynchronously.
            spawn_blocking(f)
        }
    }

    unsafe impl Blocker for Compio {
        fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T {
            // If we are already inside a compio runtime, drive `f` on it.
            // Otherwise `try_with_current` hands the closure back in `Err`,
            // and we create a fresh runtime to run it on.
            match compio::runtime::Runtime::try_with_current(|rt| rt.block_on(f)) {
                Ok(v) => v,
                Err(run) => {
                    let rt =
                        compio::runtime::Runtime::new().expect("failed to create compio runtime");
                    run(&rt)
                }
            }
        }
    }
}
