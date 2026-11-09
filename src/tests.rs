macro_rules! test_fixtures {
    // Паттерн 1: Тестовые функции.
    // Они строго имеют пустые скобки `()` после имени.
    (
        $(#[$meta:meta])*
        async fn $name:ident () $body:block
        $($rest:tt)*
    ) => {
        // smol: `smol_macros::test!` is a declarative macro; it wraps
        // the test body in a self-contained smol executor.
        #[cfg(feature = "use-smol")]
        ::smol_macros::test! {
            $(#[$meta])*
            async fn $name() $body
        }

        #[cfg(all(feature = "use-tokio", not(feature = "use-smol")))]
        $(#[$meta])*
        #[tokio::test(flavor = "multi_thread", worker_threads=1)]
        async fn $name() $body

        // Рекурсивный вызов для обработки остальных элементов
        test_fixtures! { $($rest)* }
    };

    // Паттерн 2: Хелпер-функции (и всё остальное).
    // Если функция не подошла под Паттерн 1 (т.е. у неё есть `<...>` или аргументы),
    // мы просто захватываем её целиком как `item` и выводим без изменений.
    // Атрибуты входят в состав `item`, поэтому отдельный повтор
    // `$(#[$meta:meta])*` здесь не нужен — иначе парсер не сможет
    // однозначно разделить меты и сам элемент.
    (
        $item:item
        $($rest:tt)*
    ) => {
        $item

        // Рекурсивный вызов
        test_fixtures! { $($rest)* }
    };

    // Базовый случай (конец списка)
    () => {};
}

#[cfg(feature = "use-smol")]
use crate::SmolScope as Scope;

#[cfg(all(feature = "use-tokio", not(feature = "use-smol")))]
use crate::TokioScope as Scope;

#[cfg(feature = "use-smol")]
fn future_value<T>(v: T) -> T {
    v
}

#[cfg(all(feature = "use-tokio", not(feature = "use-smol")))]
fn future_value<T>(v: Result<T, tokio::task::JoinError>) -> T {
    v.expect("join error while unwrapping value")
}

// Runtime-agnostic timeout: resolves to `Ok(value)` when `fut`
// completes, or to `Err(())` when `dur` elapses. Uses `futures-timer`,
// so it works with any executor.
test_fixtures! {
    // `allow(dead_code)`: used only by the runtime-gated tests, so
    // building tests without any spawner feature flags it as unused.
    #[allow(dead_code)]
    async fn timeout<T, F: std::future::Future<Output = T>>(dur: std::time::Duration, fut: F) -> Result<T, ()> {
        use futures::future::{select, Either};
        use futures_timer::Delay;
        use std::pin::Pin;

        let mut fut: Pin<Box<F>> = Box::pin(fut);
        let mut delay = Box::pin(Delay::new(dur));

        match select(&mut fut, &mut delay).await {
            Either::Left((v, _)) => Ok(v),
            Either::Right((_, _)) => Err(()),
        }
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

    // Рекурсивный `scope_and_block` требует возможности высвободить
    // текущий поток (как делает tokio в `block_in_place`), поэтому
    // этот тест запускается только под tokio: глобальный executor
    // smol работает на одном фоновом потоке (SMOL_THREADS=1 по
    // умолчанию), и каждый уровень рекурсии блокирует свой поток,
    // требуя новый поток для следующего уровня — см. dead-lock
    // замечание для `SmolScope` в документации lib.rs.
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

#[cfg(all(test, feature = "use-compio"))]
mod use_compio {
    use super::timeout;
    use crate::CompioScope as Scope;
    use std::any::Any;

    fn future_value<T>(v: Result<T, Box<dyn Any + Send>>) -> T {
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
