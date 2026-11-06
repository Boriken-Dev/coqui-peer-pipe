# `coqui-transport-pipe` — API header

Header-file-style reference for the `coqui-transport-pipe` crate: every public item with its signature,
contract and gotchas, so the crate can be used without reading its source. It ships inside the
package and is self-contained. Development documentation lives with the source at
<https://github.com/Boriken-Dev/coqui-transport-pipe>.

| Feature | Adds | Needed for | Default |
| --- | --- | --- | --- |
| `tokio` | `tokio` (`net`, `io-util`, `time`), `coqui-peer/tokio` | module `asynch` | off |
| `channel` | `coqui-web-channel` | module `channel`: a `coqui_web_channel::Channel`, so `coqui-agentic-peer` links run on a stream | off |

MSRV Rust 1.89. A default build depends on `coqui-peer` (by git revision, `1f00bfe`), `tracing`, on
Windows `windows-sys`, and on Unix `socket2` and `libc`. Version `0.0.0-alpha.0`, unreleased: depend on a
git revision, and name the same `coqui-peer` and `coqui-web-channel` revisions if you use them directly. This is a
transport of `coqui-peer`, not a method: it proves no key, and a method runs over it.

Imports: the crate root holds `PipeName`, `Listener`, `ListenerOptions`, `Stream`, `ConnectOptions`,
`OsIdentity`, `current_user`, `Messages`, `Error`, the `DEFAULT_` constants, and re-exports of
`coqui-peer`'s `MessageTransport`, `ReadTimeout`, `WriteTimeout` and `Weakening`. Public modules: `asynch`
(feature `tokio`), `channel` (feature `channel`).

`unsafe` only for the operating system's calls, in `src/sys/windows.rs` and `src/sys/unix_os.rs`; a test
lists them. No process-wide state. A library prints nothing: a setting looser than its default is
returned by `weakened()` and logged at `warn` through `tracing`. Nothing reads a file or an environment
variable unless the caller asks: `PipeName::named` and parsing `pipe:NAME` read two on Unix.

Tested on Windows, Linux and FreeBSD (14.5: `getpeereid`, which gives no process there). The macOS path (`getpeereid`, `LOCAL_PEERPID`) is written and has not been
run.

## Names (`coqui_transport_pipe::PipeName`)

```rust
pub struct PipeName;   // Clone, Debug, Eq, Hash, Display, FromStr, AsRef<Path>, Send, Sync
impl PipeName {
    pub fn named(name: &str) -> Result<PipeName, Error>;       // Windows \\.\pipe\NAME; Unix $XDG_RUNTIME_DIR/NAME.sock, else $TMPDIR/NAME.sock
    pub fn named_in(directory: impl AsRef<Path>, name: &str) -> Result<PipeName, Error>;   // Unix DIRECTORY/NAME.sock; Windows ignores directory
    pub fn at(path: impl AsRef<Path>) -> Result<PipeName, Error>;      // a socket path; on Windows \\.\pipe\NAME or \\HOST\pipe\NAME
    pub fn on_host(host: &str, name: &str) -> Result<PipeName, Error>; // Windows \\HOST\pipe\NAME; Error::Unsupported on Unix
    pub fn path(&self) -> &Path;
}
```

`name` is one or more of `A-Z a-z 0-9 . _ -` and does not start with a dot. `at` on Windows takes a `NAME`
that may group with `\`, without `/`, a control character, a `.` or `..` part, or a part ending in a dot or a
space (which Win32 would drop, giving one pipe two names), and a `HOST` that is `.` or holds only
`A-Z a-z 0-9 . - : _ [ ] %`; anything else is `Error::InvalidName`. `at` on Unix refuses an empty path, one with no file name, or one too long for `sun_path`, counting
the 11 bytes a bind's private directory adds. On Windows a name is held in ASCII lower case, since the system
compares them without case: two cases are one equal value.

Text: `pipe:NAME` (on Windows any `NAME` `at` takes, `GROUP\NAME` included), `unix:PATH`, or a whole path (`\\.\`
on Windows, `/` on Unix). **Text never names another host**: `\\HOST\pipe\NAME` is `Error::InvalidName` from
`FromStr`, since opening it sends this user's credentials to that host; `at` and `on_host` name one on purpose.
`Display` is canonical and parses back, but for a Unix path that is not UTF-8 and another host's pipe. `unix:`
on Windows is `Error::Unsupported`.

Environment: the first of `XDG_RUNTIME_DIR` and `TMPDIR` that holds an absolute path; neither is
`Error::NoRuntimeDirectory`. The bind then judges that directory like any other, so `TMPDIR=/tmp` is
refused.

## Listening (`coqui_transport_pipe::{Listener, ListenerOptions}`)

```rust
pub struct Listener;   // Debug, Send, Sync
impl Listener {
    pub fn bind(name: &PipeName) -> Result<Listener, Error>;
    pub fn options() -> ListenerOptions;
    pub fn accept(&self) -> Result<Stream, Error>;                                        // waits forever
    pub fn accept_timeout(&self, timeout: Option<Duration>) -> Result<Stream, Error>;     // Error::TimedOut; the listener goes on
    pub fn name(&self) -> &PipeName;
    pub fn weakened(&self) -> &[Weakening];
}
pub struct ListenerOptions;   // Clone, Debug, Default = new(), must_use; every method takes and returns self but the last two
impl ListenerOptions {
    pub fn new() -> ListenerOptions;
    pub fn allow_remote_clients(self, bool);            // Windows; default false: PIPE_REJECT_REMOTE_CLIENTS. Weakening
    pub fn security_descriptor(self, sddl: impl Into<String>);   // Windows; default: this user alone. Weakening
    pub fn first_instance(self, bool);                  // Windows; default true: FILE_FLAG_FIRST_PIPE_INSTANCE. Weakening
    pub fn max_instances(self, u32);                    // Windows; 2..=254, 255 (default) is the system's own limit
    pub fn buffer_size(self, u32);                      // Windows; each direction, default DEFAULT_BUFFER_SIZE
    pub fn socket_mode(self, u32);                      // Unix; default 0o600, must hold 0o600, at most 0o777. Looser is a weakening
    pub fn allow_shared_directory(self, bool);          // Unix; default false. Weakening
    pub fn remove_stale(self, bool);                    // Unix; default true
    pub fn backlog(self, i32);                          // Unix; above 0, default DEFAULT_BACKLOG
    pub fn read_timeout(self, Option<Duration>);        // accepted streams; default Some(DEFAULT_IO_TIMEOUT); longer is a weakening
    pub fn write_timeout(self, Option<Duration>);       // the same
    pub fn weakened(&self) -> Vec<Weakening>;           // on this platform
    pub fn bind(self, name: &PipeName) -> Result<Listener, Error>;   // Error::InvalidOptions for what means nothing
}
pub const DEFAULT_IO_TIMEOUT: Duration;      // 30 s
pub const DEFAULT_CONNECT_TIMEOUT: Duration; // 5 s
pub const DEFAULT_BUFFER_SIZE: u32;          // 64 KiB
pub const DEFAULT_BACKLOG: i32;              // 128
pub const DEFAULT_SOCKET_MODE: u32;          // 0o600
```

**On Windows**, `bind` creates the first instance and the one after it with the descriptor
`O:<SID>D:P(A;;GA;;;<SID>)`, for the user this thread runs as, plus, when its token is above medium integrity
(elevated), the label `S:(ML;;NWNRNX;;;<its level>)`, so a process of the same user below that level (not
elevated, or sandboxed) can neither connect nor add instances. `security_descriptor` replaces all of it: no label
is added, a text with no DACL gets the token's default DACL, a NUL in it is `Error::InvalidDescriptor`, and so is
one that does not let this user add instances (seen at `bind`). A name that exists is `Error::NameTaken` (the
system's `ERROR_ACCESS_DENIED`), whoever made it; with `first_instance(false)` the pipe joined keeps its own
descriptor. One instance always waits for the next client, and the next is made before a client is taken, so the
name is never free while the listener lives. Callers of `accept` on one listener wait in turn, and
`accept_timeout`'s bound covers that wait. Clients through file sharing (`\\HOST\pipe\NAME`) are refused unless
`allow_remote_clients`, and reach the system's own check as well: the descriptor applies to them too.

**On Unix**, `bind`, in this order:

1. judges the parent directory, both as written (not following links, since clients open that name) and resolved:
   refuses (`Error::SharedDirectory`) when others can write the socket's directory, sticky or not, or a directory
   above it that is not sticky; refuses (`Error::ForeignDirectory`) when a user other than this one or root owns
   any of them, or a link on the way as written. `allow_shared_directory` skips this;
2. takes an advisory lock on `<path>.lock` (mode `0600`, no symbolic link followed), held as long as the listener:
   already held is `Error::NameTaken`. The file stays when the listener ends;
3. looks at the path, not following a link. Not a socket is `Error::NotASocket`, and it is left alone. A socket
   of another user is `Error::NameTaken`. A socket something answers on is `Error::NameTaken`. Otherwise it is
   stale and removed, unless `remove_stale(false)`, then `Error::NameTaken`;
4. binds in a private directory beside it, sets the mode, then links it to the path, so the socket is never seen
   with another mode, and removes the directory.

Dropping the listener removes the path if it is still this socket.

## Streams (`coqui_transport_pipe::{Stream, ConnectOptions}`)

```rust
pub struct Stream;     // Debug, Send, Sync
impl Stream {
    pub fn connect(name: &PipeName) -> Result<Stream, Error>;
    pub fn options() -> ConnectOptions;
    pub fn peer(&self) -> io::Result<OsIdentity>;
    pub fn shutdown(&self) -> io::Result<()>;
}
impl Read + Write + ReadTimeout + WriteTimeout for Stream;
impl Read + Write + ReadTimeout + WriteTimeout for &Stream;
pub struct ConnectOptions;   // Clone, Debug, Default = new(), must_use
impl ConnectOptions {
    pub fn new() -> ConnectOptions;
    pub fn timeout(self, Duration);                 // default DEFAULT_CONNECT_TIMEOUT: for a free instance or a place in the queue
    pub fn allow_impersonation(self, bool);         // Windows; default false: SECURITY_IDENTIFICATION. Weakening
    pub fn read_timeout(self, Option<Duration>);    // default Some(DEFAULT_IO_TIMEOUT); longer is a weakening
    pub fn write_timeout(self, Option<Duration>);   // the same
    pub fn weakened(&self) -> Vec<Weakening>;
    pub fn connect(self, name: &PipeName) -> Result<Stream, Error>;
}
```

`connect` errors: `NotListening` (at once), `AccessDenied` (this user or, through file sharing, this client),
`TimedOut`, `Io`. Nothing proves who listens: run a key proof over the stream.

Every duration in this crate, options and calls alike, that is too long for the clock (`Duration::MAX`) waits as
`None` would. On Unix a write to a peer that left is `BrokenPipe` and never raises SIGPIPE, whatever the
process's disposition (`MSG_NOSIGNAL`, `SO_NOSIGPIPE` on Apple's systems).

A read past its timeout is `TimedOut`, on both platforms, and loses nothing. `Ok(0)` is the peer's end, and what
the peer wrote before closing is read first. `flush` does nothing: a write is on its way when it returns. A
timeout of zero is `InvalidInput`, as on a socket; `None` waits forever. **A write past its timeout on Windows
refuses every later write with `BrokenPipe`**: a cancelled pipe write reports 0 bytes even when the peer has
taken part of it (measured: 102,400 of 1 MiB read, 0 reported), so the stream's position is unknown. On Unix a
write reports what it wrote and `TimedOut` means nothing was.

`shutdown` ends both directions: a read or a write blocked in another thread returns, the read with `Ok(0)` and the
write with `BrokenPipe`, as do later ones; the peer reads what was written, then the end. Dropping does the
same.

**Drivers.** `drive_stream`, `complete_stream` and their `_until` forms take `&mut Stream` or `&mut &Stream`. The
`_until` forms clear both timeouts (`None`) before returning, as `coqui-peer` documents: set them again if the
stream goes on. `Method::respond` takes `None` as its source, so every peer on pipes shares one
bucket of a `TrustStore`'s waiting queue.

## Who is at the other end (`coqui_transport_pipe::{OsIdentity, current_user}`)

```rust
#[non_exhaustive] pub struct OsIdentity {
    pub process: Option<u32>,      // None for a client on another host
    pub user: Option<String>,      // a SID on Windows, a decimal uid on Unix
    pub group: Option<String>,     // Unix: a decimal gid
    pub session: Option<u32>,      // Windows
    pub host: Option<String>,      // Windows: a client's host, when it is another one
}
impl OsIdentity { pub fn is_current_user(&self) -> io::Result<Option<bool>>; }   // None when the system did not say
pub fn current_user() -> io::Result<String>;
```

Information for a log or a host's policy; it never replaces a key proof. On Unix: `SO_PEERCRED` on Linux,
`getpeereid` and `LOCAL_PEERPID` on macOS, the credentials at connect time. A Windows server: the client's process
and session from the pipe, its user from its token, read on a thread of its own by impersonating it at the level
the client allowed. That needs at least identification: a client of another program that sent no quality of
service and no data yet gives `user: None`, and so does one at anonymous level. A Windows client: the server's
process and session, and the owner of the pipe object, which only its creator (or a privileged process) chooses.
Neither says the integrity level: a sandboxed or non-elevated process of this user is reported as this user.

## Messages (`coqui_transport_pipe::Messages`)

```rust
pub const DEFAULT_MAX_MESSAGE: usize;   // coqui_peer::MAX_MESSAGE, 1 MiB
pub struct Messages<S>;                 // Debug
impl<S> Messages<S> {
    pub fn new(io: S) -> Messages<S>;
    pub fn with_max_message(self, bytes: usize) -> Messages<S>;   // both ways
    pub fn max_message(&self) -> usize;
    pub fn weakened(&self) -> Vec<Weakening>;                     // above the default
    pub fn get_ref(&self) -> &S;
    pub fn into_inner(self) -> S;                                 // a frame part read is dropped
}
impl<S: Read + Write + ReadTimeout + WriteTimeout> MessageTransport for Messages<S>;
impl<S: ReadTimeout> ReadTimeout for Messages<S>;
impl<S: WriteTimeout> WriteTimeout for Messages<S>;
```

Four big-endian length bytes, then the message: `coqui_peer::write_frame`'s format, with a bound of your own.
`send` above the bound is `InvalidInput` and writes nothing. `recv` of a declared length above it is `InvalidData`
before anything of it is read, and for every later call; the buffer grows as bytes arrive. `recv` at the stream's
end is `UnexpectedEof`, inside a frame or not.

A timeout set on `Messages` bounds a whole `send` or `recv`, however many reads or writes it takes, so a peer
sending a byte at a time cannot outlast it; one set on the stream only applies per read or write. A `recv` that timed
out keeps what arrived for the next. A `send` that stopped inside a frame refuses every later send with `BrokenPipe`.
`drive_messages_until` sets these, so its deadline is one.

A frame above the bound is `DriveError::Io(InvalidData)` from `drive_messages`, not `DriveError::TooLong`, which
`coqui-peer` reserves for its own frame functions.

## Async (`coqui_transport_pipe::asynch`, feature `tokio`)

```rust
pub struct Listener;
impl Listener {
    pub fn bind(name: &PipeName) -> Result<Listener, Error>;
    pub fn bind_with(name: &PipeName, options: ListenerOptions) -> Result<Listener, Error>;
    pub async fn accept(&mut self) -> Result<Stream, Error>;   // cancelling loses nothing
    pub fn name(&self) -> &PipeName;
    pub fn weakened(&self) -> &[Weakening];
}
pub struct Stream;     // AsyncRead + AsyncWrite + Unpin + Send
impl Stream {
    pub async fn connect(name: &PipeName) -> Result<Stream, Error>;
    pub async fn connect_with(name: &PipeName, options: ConnectOptions) -> Result<Stream, Error>;
    pub fn peer(&self) -> io::Result<OsIdentity>;
}
pub struct Messages<S>;   // new, with_max_message, max_message, weakened, get_ref, into_inner, as above
impl<S: AsyncRead + AsyncWrite + Unpin + Send> coqui_peer::asynch::MessageTransport for Messages<S>;
```

The same checks and instances as the blocking forms. Needs a tokio runtime with I/O enabled, and with time for
`_until` drivers and the waits for a free instance; tokio panics otherwise. These streams have no timeouts of their
own, and the options' read and write timeouts mean nothing here: wrap a call in `tokio::time::timeout`, or use
`coqui_peer::asynch`'s `_until` drivers. A cancelled `recv` keeps what arrived. A `send` that failed or was cancelled
after it began refuses every later send. `poll_shutdown` ends the writing direction on Unix and does nothing on
Windows, where dropping the stream is the end.

## Channel (`coqui_transport_pipe::channel`, feature `channel`)

```rust
pub const DEFAULT_QUEUED_BYTES: usize;      // 64 MiB
pub const DEFAULT_SEND_LIMIT: Duration;     // 30 s
pub struct ChannelOptions;                  // Clone, Copy, Debug, Default = new(), must_use
impl ChannelOptions {
    pub fn new() -> ChannelOptions;
    pub fn max_message(self, bytes: usize);     // both ways, default DEFAULT_MAX_MESSAGE
    pub fn queued_bytes(self, bytes: usize);
    pub fn send_limit(self, limit: Duration);
    pub fn weakened(&self) -> Vec<Weakening>;
    pub fn start(self, stream: Stream) -> io::Result<Channel>;
}
pub struct Channel;                         // Debug; implements coqui_web_channel::Channel
impl Channel {
    pub fn start(stream: Stream) -> io::Result<Channel>;   // one thread of its own, joined on close
    pub fn options() -> ChannelOptions;
    pub fn peer(&self) -> io::Result<OsIdentity>;
}
```

It is not authenticated: run a key proof on it (`coqui_agentic_peer::auth::prove`, then `Sealed`) before anything
else. `send` queues, and its writer thread writes: each queued message counts its frame and 64 bytes more against
`queued_bytes`, so many small messages are bounded as a few large ones are. `Failure::Protocol` above the bound; `Failure::NoProgress` at once
when the queue has no room; `send_timeout` waits for room, and `Failure::Timeout` means not sent, with the channel
going on. A peer that takes nothing for `send_limit` ends the channel with `Failure::NoProgress`. `receive` reads on the
caller's thread, so nothing is read ahead and a slow reader holds the peer back: `Ok(None)` on an end between frames,
`Failure::PathLost` on one inside a frame, `Failure::Timeout` keeping what arrived, `Failure::Protocol` above the
bound, which ends the connection. `set_max_message`, `set_max_receive`, `set_max_receive_after` and `set_max_send`
work as the trait says. `close`, and dropping, send what is queued for at most `send_limit`, end the connection and
join the thread. There are no datagrams and no certificates.

## Errors

`Error` (non-exhaustive): `InvalidName`, `Unsupported`, `NoRuntimeDirectory`, `InvalidOptions`, `NameTaken`,
`NotASocket`, `SharedDirectory`, `ForeignDirectory`, `InvalidDescriptor`, `NotListening`, `AccessDenied`, `TimedOut`,
`Io(io::Error)`. `Display` is one lowercase clause and quotes nothing. `From<io::Error>`, and `From<Error> for io::Error`
with a matching kind.

## Environment variables

Only for `PipeName::named` and `pipe:NAME` on Unix, read when called: `XDG_RUNTIME_DIR`, then `TMPDIR`, each counted
only when absolute.

## Gotchas

- A pipe name on Windows is visible to every user, and a socket path to whoever can search its directory. A
  squatter makes `bind` fail and cannot take a client from a listener that bound first, but a client that connects
  before any listener bound reaches the squatter: only the key proof protects it. `Stream::peer` says who owns the
  pipe.
- Windows: `\\HOST\pipe\NAME` needs the Server service on that host. Refused and denied both come back as
  `Error::AccessDenied`. Opening one is not bounded by `ConnectOptions::timeout`: the system waits for the network
  (measured: 21 s for an address that does not answer), and the async `connect` waits on its runtime thread.
- Windows: two clients can wait connected before an `accept`, on the waiting instance and the next; another then
  waits in `connect` until an `accept` makes room. There is no backlog setting.
- Unix: the bind's liveness probe takes a refused connect for a stale socket. A system that refuses a connect to a
  full queue (FreeBSD; not measured on macOS) can therefore make another program's live, saturated socket look
  stale; this crate's own listeners are protected by `<path>.lock`.
- `<path>.lock` stays beside a Unix socket by design: removing it would let two binders hold locks on two files.
- A key-proof handshake and these streams have separate bounds: the stream's 30 s read timeout ends a silent
  `drive_stream`, and `_until` then replaces it.
- A long-lived stream read with no message pending gets `TimedOut` every 30 s by default: read again, or set `None`.
