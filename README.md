# coqui-transport-pipe

[![Version](https://img.shields.io/crates/v/coqui-transport-pipe.svg)](https://crates.io/crates/coqui-transport-pipe)
[![Rust version](https://img.shields.io/crates/msrv/coqui-transport-pipe.svg)](https://crates.io/crates/coqui-transport-pipe)
[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://github.com/Boriken-Dev/coqui-transport-pipe/blob/main/LICENSE)
[![Docs](https://img.shields.io/docsrs/coqui-transport-pipe)](https://docs.rs/coqui-transport-pipe)
[![CI](https://img.shields.io/github/actions/workflow/status/Boriken-Dev/coqui-transport-pipe/test.yml)](https://github.com/Boriken-Dev/coqui-transport-pipe/actions/workflows/test.yml)

**Windows named pipes and Unix domain sockets as a [`coqui-peer`](https://github.com/Boriken-Dev/coqui-peer)
transport, with the operating system's access control on by default.** Every `coqui-peer` driver and every
method runs over it unchanged, and with the `channel` feature so does a `coqui-agentic-peer` link. It proves no
key: a method does that over the stream, and what the operating system says about the other end is reported
beside it. See the [documentation](https://boriken-dev.github.io/coqui-transport-pipe/).

> **Status: alpha.** `0.0.0-alpha.0` is unreleased: depend on a git revision. The API may change at any commit.
> Tested on Windows, Linux and FreeBSD; the macOS path is written and has not been run.

## Features

- **One API over both platforms**: `PipeName`, `Listener::bind` and `accept`, `Stream::connect`.
- **Who may connect, by default**: on Windows a descriptor for the current user alone, a name that exists is
  refused rather than joined, clients on other hosts are refused, and a client connects at the identification level,
  so a server cannot act as it. On Unix a `0600` socket from the moment its path exists, in a directory only its owner
  can write, with a socket left behind removed only when nothing listens on it.
- **Your choice, and visible**: every default can be loosened on a builder; what is looser is listed by `weakened()`
  and logged at `warn` through `tracing`.
- **Every `coqui-peer` driver**: `Read + Write`, `ReadTimeout` and `WriteTimeout` for streams, so deadlines hold;
  `AsyncRead + AsyncWrite` under `tokio`; `Messages`, a `MessageTransport` with a bound on message size.
- **`coqui-web-channel` channels**: a `Channel` over a stream, so `coqui-agentic-peer` links run on it.
- **The other end's OS identity** (`SO_PEERCRED`, the client's token), as information, never in place of a key proof.

## Installation

```bash
cargo add coqui-transport-pipe --git https://github.com/Boriken-Dev/coqui-transport-pipe
```

| Feature | Adds | Needed for | Default |
| --- | --- | --- | --- |
| `tokio` | `tokio`, `coqui-peer/tokio` | module `asynch`: async listener, stream and messages | off |
| `channel` | `coqui-web-channel` | module `channel`: a `coqui_web_channel::Channel`, for `coqui-agentic-peer` links | off |

Rust 1.89 or later. A default build depends on `coqui-peer`, `tracing`, on Windows `windows-sys`, and on Unix
`socket2` and `libc`.

## Quick start

Bytes between two ends; `PipeName::named("my-service")` is the usual name:

```rust
use std::io::{Read, Write};
use coqui_transport_pipe::{Listener, PipeName, Stream};

# let directory = tempfile::tempdir()?;
# let name = format!("readme-bytes-{}", std::process::id());
# let name = PipeName::named_in(directory.path(), &name)?;
let listener = Listener::bind(&name)?;
let mut client = Stream::connect(&name)?;
let mut server = listener.accept()?;
client.write_all(b"hello")?;
let mut buffer = [0; 5];
server.read_exact(&mut buffer)?;
assert_eq!(&buffer, b"hello");
assert_eq!(server.peer()?.is_current_user()?, Some(true));
# Ok::<(), Box<dyn std::error::Error>>(())
```

A key proof over it, with a deadline, here by Noise:

```rust
use std::time::{Duration, Instant};
use coqui_peer::{Method, drive_stream_until};
use coqui_peer_noise::{Identity, Noise, Trust};
use coqui_transport_pipe::{Listener, PipeName, Stream};

# let directory = tempfile::tempdir()?;
# let name = format!("readme-noise-{}", std::process::id());
# let name = PipeName::named_in(directory.path(), &name)?;
let (ours, theirs) = (Identity::generate()?, Identity::generate()?);
let (our_id, their_id) = (ours.peer_id(), theirs.peer_id());
let dialling = Noise::new(ours).with_responders(Trust::pinned([their_id]));
let answering = Noise::new(theirs).with_initiators(Trust::pinned([our_id]));

let listener = Listener::bind(&name)?;
let mut client = Stream::connect(&name)?;
let mut server = listener.accept()?;
let until = Instant::now() + Duration::from_secs(10);
let served = std::thread::spawn(move || {
    drive_stream_until(answering.respond(None).unwrap(), &mut server, until).map(|done| done.peer)
});
let established = drive_stream_until(dialling.initiate()?, &mut client, until)?;
assert_eq!(established.peer, their_id);
assert_eq!(served.join().unwrap()?, our_id);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Looser settings are a choice, and are reported:

```rust
use coqui_transport_pipe::Listener;

let options = Listener::options().read_timeout(None);
assert_eq!(options.weakened()[0].setting, "read_timeout");
```

## API overview

| Module | Purpose |
| --- | --- |
| `coqui_transport_pipe` | `PipeName`, `Listener`, `ListenerOptions`, `Stream`, `ConnectOptions`, `OsIdentity`, `Messages`, `Error`, and `coqui-peer`'s `MessageTransport`, `ReadTimeout`, `WriteTimeout` and `Weakening`. |
| `coqui_transport_pipe::asynch` | The listener, the stream and `Messages` over tokio (`tokio`). |
| `coqui_transport_pipe::channel` | `Channel`, a `coqui_web_channel::Channel` over a stream (`channel`). |

## Limits

- A browser cannot reach a pipe or a socket: this is for native clients.
- Name squatting: a listener that bound first cannot be displaced, but a client that connects to a name nobody
  legitimate holds yet reaches whoever does. The key proof is what protects it.
- On Windows a write past its timeout refuses every later write on that stream, since a cancelled pipe write does not
  say how much the peer took. On Unix a write reports what it wrote.
- `<path>.lock` stays beside a Unix socket when its listener ends.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +nightly-2026-09-21 public-api --all-features -sss --color never | diff -u tools/public-api.txt -
```

Run these on Windows and on a Unix: the two share an API and little code. Tests that run a client as another user
need root; `CONTRIBUTING.md` in the repository says how.

### Releasing

This project follows [Semantic Versioning](https://semver.org/) and keeps a
[`CHANGELOG.md`](https://github.com/Boriken-Dev/coqui-transport-pipe/blob/main/CHANGELOG.md).
Pushing a tag matching `v*` runs the release workflow: the tests and a strict documentation build gate a GitHub
release, marked as a pre-release when the version has a `-` suffix. Nothing is published to crates.io while the crate
depends on its family by git revision.

## License

MPL-2.0; see [LICENSE](https://github.com/Boriken-Dev/coqui-transport-pipe/blob/main/LICENSE).
