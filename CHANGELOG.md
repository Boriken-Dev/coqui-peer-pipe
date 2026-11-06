# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `PipeName`: a validated Windows pipe name or Unix socket path, written `pipe:NAME` or `unix:PATH`.
- `Listener` and `Stream`: blocking listen, accept and connect, one API over named pipes and Unix
  domain sockets. A stream implements `Read`, `Write`, `coqui_peer::ReadTimeout` and `WriteTimeout`, so
  every `coqui_peer` driver works on it, deadlines included.
- Access control on by default. Windows: a descriptor for the current user only, labelled at the
  listener's integrity level when it is elevated, so a non-elevated or sandboxed process of the same
  user can neither connect nor add instances; `FILE_FLAG_FIRST_PIPE_INSTANCE`,
  `PIPE_REJECT_REMOTE_CLIENTS`, and clients at `SECURITY_IDENTIFICATION`. Unix: a `0600` socket from
  the moment its path exists, directories others can write refused along the path as written and as
  resolved, links on the way of another user refused, a socket left behind removed only when nothing
  listens on it. Every setting looser than its default is listed by `weakened()` and logged at `warn`
  through `tracing`.
- `PipeName` text names this host only: `\\HOST\pipe\NAME` is made with `PipeName::at` or `on_host`.
  Windows names Win32 would rewrite (a trailing dot or space, `\\?\`) are refused.
- A duration too long for the clock (`Duration::MAX`) waits as `None` does, everywhere.
- On Unix a write to a peer that left is `BrokenPipe`, never SIGPIPE, on every supported Rust.
- `OsIdentity`: what the operating system says about the other end, as information.
- `Messages`: a `MessageTransport` of length-framed messages with a bound on their size.
- `asynch`, behind `tokio`: the listener, the stream (`AsyncRead + AsyncWrite`) and `Messages` over
  tokio.
- `channel`, behind `channel`: a `coqui_web_channel::Channel` over a stream, so `coqui-agentic-peer` links run
  on a pipe.
- `Connector`, and `asynch::Connector` with `tokio`: a name and `ConnectOptions` as a `coqui_transport::Connector` and
  `AsyncConnector`; `Listener` is a `coqui_transport::Listener`.

### Changed

- The crate is `coqui-transport-pipe` (it was `coqui-peer-pipe`), in the `coqui-transport` family: the repository is
  `Boriken-Dev/coqui-transport-pipe`, the library `coqui_transport_pipe`.
- The `channel` feature is no longer a module: it makes `Stream` a `coqui_transport::Split` (with `ReadHalf` and
  `WriteHalf`), so `coqui_transport::StreamChannel::start(stream, PeerFrame)` is the channel on a pipe. The wire is
  unchanged, byte for byte (a test reads `coqui-transport`'s `peer_frame.json` through a real pipe), and a
  `Messages` and a `StreamChannel` talk to each other.
- `Messages`, blocking and async, is framed by `coqui_transport::frame::PeerFrame`, and
  `DEFAULT_MAX_MESSAGE` is `coqui_transport::frame::DEFAULT_MAX_MESSAGE` (still 1 MiB). A `recv` now reads ahead of
  the frame it returns, so `into_inner` drops a frame already read behind it, not only a part of one.
- The crate depends on `coqui-transport` by git revision, and no longer on `coqui-web-channel` or, for its tests, on
  `coqui-agentic-peer`.

### Removed

- The `channel` module: `Channel`, `ChannelOptions`, `DEFAULT_QUEUED_BYTES` and `DEFAULT_SEND_LIMIT`. Use
  `coqui_transport::StreamChannel` (its builder has the same options and more); `Channel::peer` is `Stream::peer`,
  asked before the stream is given to the channel. The channel read inside `receive`; `StreamChannel` reads ahead on a
  thread (`set_pending` bounds it).
- The channel's own frame code. `Messages` stays: it is `coqui-peer`'s `MessageTransport`, which no `coqui-transport`
  type is, and it keeps a bound of its own and a deadline over a whole receive.
