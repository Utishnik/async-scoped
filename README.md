# Async-scoped

Enables controlled spawning of non-`'static` futures when
using the [smol](//github.com/smol-rs/smol),
[tokio](//github.com/tokio-rs/tokio) or
[compio](//github.com/compio-rs/compio) executors.

## Motivation

Present executors (such as smol, tokio, compio, etc.) all
support spawning `'static` futures onto a thread-pool.
However, they do not support spawning futures with lifetime
smaller than `'static`.

While the future combinators such as `for_each_concurrent`
offer concurrency, they are bundled as a single `Task`
structure by the executor, and hence are not driven
in parallel. This can be seen when benchmarking a reasonable
number (> ~1K) of I/O futures, or a few CPU heavy futures.

## Usage

The API is meant to be a minimal wrapper around efficient
executors. Users may use "use-smol", "use-tokio", or the
"use-compio" features, to obtain a specific global executor implementation.
These features provide `SmolScope`, `TokioScope` and `CompioScope` that
support spawning, and blocking.
However, none of those features are necessary -
you may freely implement your own executor. See
[docs.rs](https://docs.rs/async-scoped) for detailed
documentation.

## License

Licensed under either of [Apache License, Version
2.0](//www.apache.org/licenses/LICENSE-2.0) or [MIT
license](//opensource.org/licenses/MIT) at your option.

Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in this crate by you,
as defined in the Apache-2.0 license, shall be dual licensed
as above, without any additional terms or conditions.
