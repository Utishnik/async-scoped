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

    thread_local! {
        /// `smol` does not provide an async-std-like blocking pool out of
        /// the box, so we back `spawn_func` with a lazily initialized
        /// `futures` thread pool.
        static BLOCKING_POOL: futures::executor::ThreadPool =
            futures::executor::ThreadPool::new();
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
            BLOCKING_POOL.with(|pool| pool.spawn_with_handle(f).expect("failed to spawn blocking function"))
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

#[cfg(all(feature = "use-compio", unix))]
pub mod use_compio {
    use super::*;
    use compio::{block_on, spawn};

    #[derive(Default)]
    pub struct Compio;

    unsafe impl<T: Send + 'static> Spawner<T> for Compio {
        type FutureOutput = std::result::Result<T, async_executor::TaskError>;
        type SpawnHandle = compio::runtime::JoinHandle<T>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            spawn(f)
        }
    }

    unsafe impl<T: Send + 'static> FuncSpawner<T> for Compio {
        type FutureOutput = T;
        type SpawnHandle = futures::future::Ready<T>;

        fn spawn_func<F: FnOnce() -> T + Send + 'static>(&self, f: F) -> Self::SpawnHandle {
            // `compio` runs on a single-threaded runtime, so there is no
            // thread pool to offload a blocking function onto. Run it
            // eagerly and wrap the result in a ready future.
            futures::future::ready(f())
        }
    }

    unsafe impl Blocker for Compio {
        fn block_on<T, F: Future<Output = T>>(&self, f: F) -> T {
            block_on(f)
        }
    }
}
