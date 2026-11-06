# coqui-transport-pipe

Windows named pipes and Unix domain sockets as a transport of
[`coqui-peer`](https://github.com/Boriken-Dev/coqui-peer), so anything that runs a coqui-peer
handshake or a `coqui-transport` channel also runs between two processes on one machine, with no port
to squat and the operating system's access control beside the key proof.

!!! note "Alpha"
    `0.0.0-alpha.0` is unreleased: depend on a git revision. The API may change in any commit.

The listener's defaults are the strict ones, and each can be loosened and is then listed as a
weakening:

- On Windows the pipe's descriptor gives the current user alone access, a name that exists is
  refused rather than joined, clients on other hosts are refused, and a client connects at the
  identification level, so a server cannot act as it.
- On Unix the socket is `0600` from the moment its path exists, directories others can write are
  refused, and a socket left behind is removed only when nothing listens on it.

The operating system's word on who connected is reported as information; it never replaces the
key proof a method runs over the stream.

The API reference is under
[Rust API](https://boriken-dev.github.io/coqui-transport-pipe/rust/index.html) for `main`. The source,
the API header (`AGENTS.md`) and the changelog are in the
[repository](https://github.com/Boriken-Dev/coqui-transport-pipe).
