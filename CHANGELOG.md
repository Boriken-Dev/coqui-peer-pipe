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
- `ListenerOptions::socket_group` and `socket_group_name` (Unix): the socket's group, set before the path exists, so
  `socket_mode(0o660)` shares it with that group alone. A group the process cannot set is `Error::GroupDenied`, a name
  nobody has is `Error::UnknownGroup`, another group than the process's own is in `weakened()`, and Windows refuses it.
- `not_applied()` on `ListenerOptions`, `Listener`, `asynch::Listener` and `ConnectOptions`.
- `Connector`, and `asynch::Connector` with `tokio`: a name and `ConnectOptions` as a `coqui_transport::Connector` and
  `AsyncConnector`; `Listener` is a `coqui_transport::Listener`.

### Changed

- The crate is `coqui-transport-pipe` (it was `coqui-peer-pipe`), in the `coqui-transport` family: the repository is
  `Boriken-Dev/coqui-transport-pipe`, the library `coqui_transport_pipe`.
- The `channel` feature is no longer a module: it makes `Stream` a `coqui_transport::Split` (with `ReadHalf` and
  `WriteHalf`), so `coqui_transport::StreamChannel::start(stream, codec)` is the channel on a pipe, with the codec the
  caller chooses.
- The crate picks no framing: the caller gives `StreamChannel` a codec. A framing is an implementation detail of
  whoever speaks it, so none lives here.
- A setting looser than its default is reported, never warned about: `weakened()` returns it (what, its default, its
  value) and the crate logs it at `debug`. Only the application knows whether a shared socket or a long timeout was
  meant.
- `socket_mode` accepts any mode up to `0o7777`, where it had to hold `0o600` and stay within `0o777`; one other than the
  default is in `weakened()` and is never refused or warned about.
- `ListenerOptions::bind` refuses, with `InvalidOptions`, a setting stricter than the default that the platform cannot
  honour (`socket_group` and `remove_stale(false)` on Windows, `security_descriptor` and `max_instances` on Unix). One
  that is meaningless there is no longer dropped silently: `not_applied()` lists it.
- The crate depends on `coqui-transport` by git revision, and no longer on `coqui-web-channel` or, for its tests, on
  `coqui-agentic-peer`.

### Removed

- The `channel` module: `Channel`, `ChannelOptions`, `DEFAULT_QUEUED_BYTES` and `DEFAULT_SEND_LIMIT`. Use
  `coqui_transport::StreamChannel` (its builder has the same options and more); `Channel::peer` is `Stream::peer`,
  asked before the stream is given to the channel. The channel read inside `receive`; `StreamChannel` reads ahead on a
  thread (`set_pending` bounds it).
- The channel's own frame code, and `Messages` (blocking and async) with `DEFAULT_MAX_MESSAGE`: they were the length
  framing. Run `coqui-peer`'s `drive_stream` over a `Stream`, or `StreamChannel` with a codec.
