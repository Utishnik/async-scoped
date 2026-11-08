macro_rules! test_fixtures {
    ($($item:item)*) => {
        $(
            #[cfg_attr(feature = "use-smol", smol::test)]
            #[cfg_attr(all(feature = "use-tokio", not( feature = "use-smol" )), tokio::test(flavor = "multi_thread", worker_threads=1))]
            $item
        )*
    }
}

cfg_smol! {
    use crate::SmolScope as Scope;

    fn future_value<T>(v: T) -> T {
        v
    }
}
cfg_smol_or_else! {
    use crate::TokioScope as Scope;

    fn future_value<T>(v: Result<T, tokio::task::JoinError>) -> T {
        v.expect("join error while unwrapping value")
    }
}


test_fixtures! {
    /// Runtime-agnostic timeout: polls `fut` in a task driven by the
    /// current executor and resolves to `Err(())` when `dur` elapses.
    async fn timeout<T, F: std::future::Future<Output = T>>(dur: std::time::Duration, fut: F) -> Result<T, ()> {
        use futures::stream::{Stream, StreamExt};
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct Timeout<F> {
            deadline: std::time::Instant,
            fut: Pin<Box<F>>,
        }
        impl<F: std::future::Future> Stream for Timeout<F> {
            type Item = Result<F::Output, ()>;
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                if let Poll::Ready(v) = self.fut.as_mut().poll(cx) {
                    return Poll::Ready(Some(Ok(v)));
                }
                if self.deadline <= std::time::Instant::now() {
                    return Poll::Ready(Some(Err(())));
                }
                Poll::Pending
            }
        }

        Timeout {
            deadline: std::time::Instant::now() + dur,
            fut: Box::pin(fut),
        }
        .next()
        .await
        .expect("Timeout stream yields exactly one item")
    }

    async fn test_scope() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let (stream, _) = unsafe { Scope::scope(|s| {
            for _ in 0..10 {
                let proc = || async move {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
            }
        })};
        // })};

        // Uncomment this for compile error
        // std::mem::drop(not_copy);

        use futures::StreamExt;
        let count = stream.collect::<Vec<_>>().await.len();

        // Drop here is okay, as stream has been consumed.
        std::mem::drop(not_copy);
        assert_eq!(count, 10);
    }

    // Test scope bounds: should allow any future with lifetime
    // larger than the scope's lifetime
    async fn scope_lifetime() {
        use std::future::Future;
        let static_fut = futures::future::ready(());
        fn test_static<F: Future + 'static>(_: &F) {}
        test_static(&static_fut);

        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;
        let ((), vals) = unsafe { Scope::scope_and_collect(|s| {
            s.spawn(static_fut);
            for _ in 0..10 {
                let proc = || async {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
            }
        })}.await;
        assert_eq!(vals.len(), 11);

    }
    async fn scope_async() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let stream = unsafe {
            use std::time::Duration;
            let mut s = Scope::create(Default::default());
            for _ in 0..10 {
                let proc = || async move {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
                let _ = timeout(Duration::from_millis(10), std::future::pending::<()>()).await;
            }
            s
        };

        // Uncomment this for compile error
        // std::mem::drop(not_copy);

        use futures::StreamExt;
        let count = stream.collect::<Vec<_>>().await.len();

        // Drop here is okay, as stream has been consumed.
        std::mem::drop(not_copy);
        assert_eq!(count, 10);
    }


    async fn test_scope_and_collect() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let (_, vals) = unsafe { Scope::scope_and_collect(|s| {
            for _ in 0..10 {
                let proc = || async {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
            }
        }) }.await;

        assert_eq!(vals.len(), 10);
    }
    async fn test_scope_and_block() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let ((), vals) = Scope::scope_and_block(|s| {
            for _ in 0..10 {
                let proc = || async {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
            }
        });

        assert_eq!(vals.len(), 10);
    }

    async fn test_scope_and_block_spawn_blocking() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let ((), vals) = Scope::scope_and_block(|s| {
            for _ in 0..10 {
                let proc = || {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn_blocking(proc);
            }
        });

        assert_eq!(vals.len(), 10);
    }

    // Check that a cancellable future works as the
    // contained future under normal circumstances.
    async fn test_cancellation_completeness() {
        use std::time::*;

        // Represents a work future
        async fn proc() -> bool {
            timeout(Duration::from_millis(100), std::future::pending::<()>())
                .await
                .is_err()
        }
        let ((), items) = Scope::scope_and_block(|scope| {
            scope.spawn_cancellable(proc(), || false);
        });
        assert_eq!(items.len(), 1);
        for i in items {
            assert_eq!(future_value(i), true);
        }
    }

    // Check that a cancellable future works as the
    // contained future under normal circumstances.
    async fn test_cancellation_works() {
        use std::time::*;

        // Represents a work future
        async fn proc() -> bool {
            timeout(Duration::from_millis(10000), std::future::pending::<()>())
                .await
                .is_err()
        }
        let ((), items) = Scope::scope_and_block(|scope| {
            scope.spawn_cancellable(proc(), || false);
            scope.cancel();
        });
        assert_eq!(items.len(), 1);
        for i in items {
            assert_eq!(future_value(i), false);
        }
    }


    // This is a simplified version of the soundness bug
    // pointed out on [reddit][reddit-ref]. Here, we test that
    // it does not happen when using the `scope_and_collect`,
    // but the returned future is not forgotten. Forgetting the
    // future should lead to an invalid memory access.
    //
    // [reddit-ref]: https://www.reddit.com/r/rust/comments/ee3vsu/asyncscoped_spawn_non_static_futures_with_asyncstd/fbpis3c?utm_source=share&utm_medium=web2x
    async fn test_cancellation_soundness() {
        use std::time::*;

        async fn inner() {
            let mut shared = true;
            let shared_ref = &mut shared;

            let start = Instant::now();

            let mut fut = Box::pin(
                unsafe { Scope::scope_and_collect(|scope| {
                    scope.spawn_cancellable(async {
                        assert!(timeout(Duration::from_millis(500), std::future::pending::<()>())
                            .await
                            .is_err());

                        eprintln!("Trying to write to shared_ref");
                        *shared_ref = false;
                        assert!(*shared_ref);
                    }, || ());
                })}
            );
            let _ = timeout(Duration::from_millis(10), &mut fut).await;

            // Dropping explicitly to measure time taken to complete drop.
            // Change the drop to forget for panic due to invalid mem. access.
            std::mem::drop(fut);
            let elapsed = start.elapsed().as_millis();


            // The cancelled future should have been polled
            // before the inner large timeout.
            assert!(elapsed < 100);
            eprintln!("Elapsed: {}ms", start.elapsed().as_millis());
        }

        inner().await;

        // This timeout allows any (possible) invalid memory
        // access to actually take place.
        assert!(timeout(Duration::from_millis(600), std::future::pending::<()>())
            .await
            .is_err());

    }

    // This test is resource consuming and ignored by default
    #[ignore]
    async fn backpressure() {
        let mut s = unsafe { Scope::create(Default::default()) };
        let limit = 0x10;
        for i in 0..0x100 {
            s.spawn(async {
                // Allocate a large array (256 MB)
                let blob = vec![42u8; 0x10000000];

                // Spend a lot of time on it asynchronously
                use std::time::Duration;
                let _ = timeout(Duration::from_millis(100), std::future::pending::<()>()).await;

                std::mem::drop(blob);
            });

            while s.remaining() > limit {
                use futures::StreamExt;
                s.next().await;
            }
            eprintln!("Spawned {} futures", i);
        }
    }

    // Mutability test: should fail to compile.
    // TODO: use compiletest_rs
    // #[smol::test]
    // async fn mutating_scope() {
    //     let mut not_copy = String::from("hello world!");
    //     let not_copy_ref = &mut not_copy;
    //     let mut count = 0;

    //     crate::scope_and_block(|s| {
    //         for _ in 0..10 {
    //             let proc = || async {
    //                 not_copy_ref.push('.');
    //             };
    //             s.spawn(proc()); //~ ERROR
    //         }
    //     });

    //     assert_eq!(count, 10);
    // }

    // https://github.com/rmanoka/async-scoped/issues/2
    // https://github.com/async-rs/async-std/issues/644
    async fn test_async_deadlock() {
        use std::future::Future;
        use futures::FutureExt;
        fn nth(n: usize) -> impl Future<Output=usize> + Send {
            eprintln!("nth({})", n);
            async move {
                let mut result: usize = 0;
                Scope::scope_and_block(|scope| {
                    if n > 0 {
                        scope.spawn(async {
                            let rec = { nth(n-1).boxed() }.await;
                            result = rec + 1;
                        });
                    }
                });
                eprintln!("nth({})={}", n, result);
                result
            }
        }
        let input = 4;
        assert_eq!(nth(input).await, input);
    }

    async fn test_ordered_collect() {
        const N: u64 = 10;

        let (_, r) = Scope::scope_and_block(|scope| {
            for i in 0..N {
                scope.spawn(async move {
                    let _ = timeout(
                        std::time::Duration::from_millis(100 - i),
                        std::future::pending::<()>(),
                    )
                    .await;
                    i
                });
            }
        });
        let r = r.into_iter().map(|v| {
            future_value(v)
        }).collect::<Vec<_>>();

        assert_eq!((0..N).into_iter().collect::<Vec<_>>(), r);
    }
}

#[cfg(feature = "use-tokio")]
// https://github.com/rmanoka/async-scoped/issues/2
// https://github.com/async-rs/async-std/issues/644
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_async_deadlock_tokio() {
    use crate::TokioScope;
    use futures::FutureExt;
    use std::future::Future;
    fn nth(n: usize) -> impl Future<Output = usize> + Send {
        // eprintln!("nth({})", n);

        async move {
            let result = if n == 0 {
                0
            } else {
                TokioScope::scope_and_block(|scope| {
                    scope.spawn(nth(n - 1).boxed());
                })
                .1[0]
                    .as_ref()
                    .unwrap()
                    + 1
            };

            // eprintln!("nth({})={}", n, result);
            result
        }
    }
    // Tokio has a block_in_place functionality, that lets
    // us recurse without deadlocks.
    let input = 200;
    assert_eq!(nth(input).await, input);
}

/// Dropping an empty scope should be a no-op.
#[test]
fn test_empty_scope() {
    use crate::spawner::{Blocker, Spawner};
    use std::future::Future;

    struct PanickingSpawner;

    unsafe impl<T: Send + 'static> Spawner<T> for PanickingSpawner {
        type FutureOutput = T;
        type SpawnHandle = futures::future::Ready<T>;

        fn spawn<F: Future<Output = T> + Send + 'static>(&self, _f: F) -> Self::SpawnHandle {
            panic!("spawn should never be called.");
        }
    }

    unsafe impl Blocker for PanickingSpawner {
        fn block_on<T, F: Future<Output = T>>(&self, _f: F) -> T {
            panic!("block_on should never be called.");
        }
    }

    let _ = unsafe { crate::Scope::<(), _>::create(PanickingSpawner) };
}

#[cfg(all(test, feature = "use-compio", unix))]
mod use_compio {
    use crate::CompioScope as Scope;

    fn future_value<T>(v: Result<T, async_executor::TaskError>) -> T {
        v.expect("join error while unwrapping value")
    }

    #[compio::test]
    async fn test_scope_and_block() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let ((), vals) = Scope::scope_and_block(|s| {
            for _ in 0..10 {
                let proc = || async {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn(proc());
            }
        });

        assert_eq!(vals.len(), 10);
    }

    #[compio::test]
    async fn test_scope_and_collect() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let (_, vals) = unsafe {
            Scope::scope_and_collect(|s| {
                for _ in 0..10 {
                    let proc = || async {
                        assert_eq!(not_copy_ref, "hello world!");
                    };
                    s.spawn(proc());
                }
            })
        }
        .await;

        assert_eq!(vals.len(), 10);
    }

    #[compio::test]
    async fn test_spawn_blocking_func() {
        let not_copy = String::from("hello world!");
        let not_copy_ref = &not_copy;

        let ((), vals) = Scope::scope_and_block(|s| {
            for _ in 0..10 {
                let proc = || {
                    assert_eq!(not_copy_ref, "hello world!");
                };
                s.spawn_blocking(proc);
            }
        });

        assert_eq!(vals.len(), 10);
    }

    #[compio::test]
    async fn test_cancellation_works() {
        use std::time::*;

        // Represents a work future
        async fn proc() -> bool {
            timeout(Duration::from_millis(10000), std::future::pending::<()>())
                .await
                .is_err()
        }
        let ((), items) = Scope::scope_and_block(|scope| {
            scope.spawn_cancellable(proc(), || false);
            scope.cancel();
        });
        assert_eq!(items.len(), 1);
        for i in items {
            assert_eq!(future_value(i), false);
        }
    }
}
