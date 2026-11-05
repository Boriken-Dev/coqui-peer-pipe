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
- Access control on by default. Windows: a descriptor for the current user only,
  `FILE_FLAG_FIRST_PIPE_INSTANCE`, `PIPE_REJECT_REMOTE_CLIENTS`, and clients at
  `SECURITY_IDENTIFICATION`. Unix: a `0600` socket from the moment its path exists, directories others
  can write refused, a socket left behind removed only when nothing listens on it. Every setting
  looser than its default is listed by `weakened()` and logged at `warn` through `tracing`.
- `OsIdentity`: what the operating system says about the other end, as information.
- `Messages`: a `MessageTransport` of length-framed messages with a bound on their size.
- `asynch`, behind `tokio`: the listener, the stream (`AsyncRead + AsyncWrite`) and `Messages` over
  tokio.
- `channel`, behind `channel`: a `coqui_web::Channel` over a stream, so `coqui-agentic-peer` links run
  on a pipe.
