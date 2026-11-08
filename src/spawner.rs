//! There you can find traits that are necessary for implementing executor.
//! They are mostly unsafe, as we can't ensure the executor allows intended manipulations
//! without causing UB.
use futures::task::SpawnExt;
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

    thread_local! {
        /// `smol` does not provide an async-std-like blocking pool out of
        /// the box, so we back `spawn_func` with a lazily initialized
        /// `futures` thread pool.
        static BLOCKING_POOL: futures::executor::ThreadPool =
            futures::executor::ThreadPool::new().unwrap();
    }

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
            BLOCKING_POOL.with(|pool| {
                pool.spawn_with_handle(f)
                    .expect("failed to spawn blocking function")
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
    /// `Err` if the task panicked (carrying the panic payload).
    pub type TaskResult<T> = std::result::Result<T, std::any::Any>;

    #[derive(Default)]
    pub struct Compio;

    unsafe impl<T: Send + 'static> Spawner<T> for Compio {
        type FutureOutput = std::result::Result<T, TaskResult<std::any::Any>>;
        type SpawnHandle = Task<std::result::Result<T, TaskResult<std::any::Any>>>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // `compio` spawns tasks onto the current (single-threaded)
            // runtime. The futures we spawn are boxed as `dyn Future +
            // Send`, which compio accepts (it simply drives them on the
            // current thread). compio's own `spawn` wraps the future in
            // `AssertUnwindSafe(fut).catch_unwind()` and returns a
            // `JoinHandle<T> = Task<Result<T, Box<dyn Any + Send>>>`.
            // We map that into our error type here instead of leaking
            // `Box<dyn Any>` out of this module.
            spawn(f).map(|res| res.map_err(|e| Err(e)))
        }
    }

    unsafe impl<T: Send + 'static> FuncSpawner<T> for Compio {
        type FutureOutput = std::result::Result<T, TaskResult<std::any::Any>>;
        type SpawnHandle = Task<std::result::Result<T, TaskResult<std::any::Any>>>;

        fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // `compio` provides `spawn_blocking`, which runs the closure
            // on a dedicated thread and delivers the result back to the
            // runtime asynchronously.
            spawn_blocking(f).map(|res| res.map_err(|e| Err(e)))
        }
    }

    unsafe impl Blocker for Compio {
        fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T {
            // If we are already inside a compio runtime, drive `f` as a
            // task on it. Otherwise, create a fresh runtime to block on
            // `f`.
            match compio::runtime::Runtime::try_with_current(|rt| {
                let handle = rt.spawn(f);
                rt.block_on(handle).expect("the spawned future panicked")
            }) {
                Ok(v) => v,
                Err(f) => compio::runtime::Runtime::new()
                    .expect("failed to create compio runtime")
                    .block_on(f),
            }
        }
    }
}
