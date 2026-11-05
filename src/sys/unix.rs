//! Unix domain sockets. Binding, access control and waiting, in safe code.
#![forbid(unsafe_code)]

use core::time::Duration;
use std::ffi::OsString;
use std::fs::{self, DirBuilder, File, OpenOptions, Permissions, TryLockError};
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::fd::AsFd;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Instant;

use socket2::{Domain, SockAddr, Socket, Type};

use super::unix_os::{READABLE, WRITABLE, effective_user, peer_credentials, wait_ready};
use crate::error::Error;
use crate::identity::OsIdentity;
use crate::name::PipeName;
use crate::options::{ConnectOptions, ListenerOptions};

/// One write at most. Linux applies a socket's send timeout to each wait for buffer room, so one large
/// write to a peer reading slowly outlasts it many times over (32 MiB, 300 ms: 3 s and more).
const MAX_WRITE: usize = 64 * 1024;
/// How often a connect looks again at a queue with no room.
const FULL_QUEUE_POLL: Duration = Duration::from_millis(5);
/// Group and other write bits.
const WRITABLE_BY_OTHERS: u32 = 0o022;
const STICKY: u32 = 0o1000;
/// Tries at a new name for the private directory of a bind.
const PRIVATE_NAME_ATTEMPTS: u32 = 64;

pub(crate) fn current_user() -> io::Result<String> {
    Ok(effective_user().to_string())
}

#[derive(Debug)]
pub(crate) struct Stream {
    socket: UnixStream,
}

impl Stream {
    pub(crate) fn new(
        socket: UnixStream,
        read: Option<Duration>,
        write: Option<Duration>,
    ) -> io::Result<Self> {
        socket.set_read_timeout(read)?;
        socket.set_write_timeout(write)?;
        Ok(Self { socket })
    }

    pub(crate) fn connect(name: &PipeName, options: &ConnectOptions) -> Result<Self, Error> {
        let deadline = Instant::now() + options.timeout;
        loop {
            if let Some(socket) = try_connect(name.path(), deadline)? {
                return Ok(Self::new(
                    socket,
                    options.read_timeout,
                    options.write_timeout,
                )?);
            }
            if Instant::now() >= deadline {
                return Err(Error::TimedOut);
            }
            std::thread::sleep(FULL_QUEUE_POLL);
        }
    }

    pub(crate) fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        // A read of nothing would otherwise block on a socket until data or a timeout.
        if buffer.is_empty() {
            return Ok(0);
        }
        (&self.socket).read(buffer).map_err(one_timeout)
    }

    pub(crate) fn write(&self, buffer: &[u8]) -> io::Result<usize> {
        let chunk = &buffer[..buffer.len().min(MAX_WRITE)];
        (&self.socket).write(chunk).map_err(one_timeout)
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket.set_read_timeout(timeout)
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket.set_write_timeout(timeout)
    }

    pub(crate) fn peer(&self) -> io::Result<OsIdentity> {
        peer_of(&self.socket)
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        match self.socket.shutdown(Shutdown::Both) {
            // The peer already ended it, which is the state asked for.
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            other => other,
        }
    }
}

/// A timed-out socket call is `WouldBlock` here and `TimedOut` on Windows; a caller sees one.
fn one_timeout(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::WouldBlock {
        io::ErrorKind::TimedOut.into()
    } else {
        error
    }
}

pub(crate) fn peer_of(socket: &impl AsFd) -> io::Result<OsIdentity> {
    let (process, user, group) = peer_credentials(socket.as_fd())?;
    Ok(OsIdentity {
        process,
        user: Some(user.to_string()),
        group: Some(group.to_string()),
        session: None,
        host: None,
    })
}

/// One attempt to connect: the stream, or `None` when the queue has no room for now.
pub(crate) fn try_connect(path: &Path, deadline: Instant) -> Result<Option<UnixStream>, Error> {
    let address = SockAddr::unix(path).map_err(|_| Error::InvalidName)?;
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    match socket.connect(&address) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(libc::EAGAIN) => return Ok(None),
        Err(error) if error.raw_os_error() == Some(libc::EINPROGRESS) => {
            let left = deadline.saturating_duration_since(Instant::now());
            if !wait_ready(socket.as_fd(), WRITABLE, Some(left))? {
                return Err(Error::TimedOut);
            }
            if let Some(error) = socket.take_error()? {
                return Err(connect_error(error));
            }
        }
        Err(error) => return Err(connect_error(error)),
    }
    socket.set_nonblocking(false)?;
    Ok(Some(socket.into()))
}

fn connect_error(error: io::Error) -> Error {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => Error::NotListening,
        io::ErrorKind::PermissionDenied => Error::AccessDenied,
        _ => Error::Io(error),
    }
}

/// What a listener holds at its path: the lock, and the socket file to remove when it ends.
#[derive(Debug)]
pub(crate) struct Bound {
    path: PathBuf,
    device: u64,
    inode: u64,
    /// Dropped after the path is removed, so nobody else can bind before that.
    _lock: File,
}

impl Drop for Bound {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|found| found.dev() == self.device && found.ino() == self.inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Refuses a directory on the way to a socket that someone else could use to replace it: the
/// socket's own directory when others can write it, one above it when others can write it and rename
/// what it holds, and any of them another user owns. Root can do all of that in any case.
fn check_directories(directory: &Path, user: u32) -> Result<(), Error> {
    for (depth, ancestor) in directory.ancestors().enumerate() {
        let metadata = fs::metadata(ancestor)?;
        if metadata.uid() != user && metadata.uid() != 0 {
            return Err(Error::ForeignDirectory);
        }
        let others_can_write = metadata.mode() & WRITABLE_BY_OTHERS != 0;
        let is_sticky = metadata.mode() & STICKY != 0;
        if others_can_write && (depth == 0 || !is_sticky) {
            return Err(Error::SharedDirectory);
        }
    }
    Ok(())
}

/// The lock of `path`, held by one listener at a time. Two binders probing one stale socket could
/// otherwise each remove what the other just bound. The file stays: a removed lock file would let
/// a waiter hold a lock on a file nobody else can see.
fn take_lock(path: &Path) -> Result<File, Error> {
    let mut lock_path = OsString::from(path);
    lock_path.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_path)?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(Error::NameTaken),
        Err(TryLockError::Error(error)) => Err(Error::Io(error)),
    }
}

/// Leaves `path` free, removing only a socket of this user that nothing listens on.
fn clear_path(path: &Path, user: u32, remove_stale: bool) -> Result<(), Error> {
    let found = match fs::symlink_metadata(path) {
        Ok(found) => found,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::Io(error)),
    };
    if !found.file_type().is_socket() {
        return Err(Error::NotASocket);
    }
    if found.uid() != user {
        return Err(Error::NameTaken);
    }
    let deadline = Instant::now() + FULL_QUEUE_POLL;
    match try_connect(path, deadline) {
        Ok(_) => Err(Error::NameTaken),
        Err(Error::NotListening) if remove_stale => match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(Error::Io(error)),
            _ => Ok(()),
        },
        Err(Error::NotListening) => Err(Error::NameTaken),
        Err(other) => Err(other),
    }
}

/// A new directory beside the socket, only this user's, removed when dropped.
struct PrivateDirectory {
    path: PathBuf,
}

impl PrivateDirectory {
    fn create(beside: &Path) -> io::Result<Self> {
        let seed = std::process::id();
        for attempt in 0..PRIVATE_NAME_ATTEMPTS {
            let path = beside.join(format!(".{:08x}", seed.wrapping_add(attempt)));
            match DirBuilder::new().mode(0o700).create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::ErrorKind::AlreadyExists.into())
    }
}

impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.path.join("s"));
        let _ = fs::remove_dir(&self.path);
    }
}

/// Binds in a private directory and links the socket to `path` once its mode is set, so nobody can
/// see or connect to it with any other mode. std and bind(2) take the mode from the umask, which
/// belongs to the whole process.
fn bind_privately(
    directory: &Path,
    path: &Path,
    options: &ListenerOptions,
) -> Result<Socket, Error> {
    let private = PrivateDirectory::create(directory)?;
    let temporary = private.path.join("s");
    let address = SockAddr::unix(&temporary).map_err(|_| Error::InvalidName)?;
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.bind(&address)?;
    fs::set_permissions(&temporary, Permissions::from_mode(options.socket_mode))?;
    socket.listen(options.backlog)?;
    fs::hard_link(&temporary, path).map_err(|error| match error.kind() {
        io::ErrorKind::AlreadyExists => Error::NameTaken,
        _ => Error::Io(error),
    })?;
    Ok(socket)
}

/// Binds `name` after the checks [`Listener`](crate::Listener) documents, returning a listener in
/// non-blocking mode and what removes its path.
pub(crate) fn bind_socket(
    name: &PipeName,
    options: &ListenerOptions,
) -> Result<(UnixListener, Bound), Error> {
    let file_name = name.path().file_name().ok_or(Error::InvalidName)?;
    let parent = match name.path().parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let directory = fs::canonicalize(parent)?;
    let user = effective_user();
    if !options.shared_directory {
        check_directories(&directory, user)?;
    }
    let path = directory.join(file_name);
    let lock = take_lock(&path)?;
    clear_path(&path, user, options.remove_stale)?;
    let socket = bind_privately(&directory, &path, options)?;
    socket.set_nonblocking(true)?;
    let bound = fs::symlink_metadata(&path)?;
    Ok((
        socket.into(),
        Bound {
            device: bound.dev(),
            inode: bound.ino(),
            path,
            _lock: lock,
        },
    ))
}

#[derive(Debug)]
pub(crate) struct Listener {
    socket: UnixListener,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    _bound: Bound,
}

impl Listener {
    pub(crate) fn bind(name: &PipeName, options: &ListenerOptions) -> Result<Self, Error> {
        let (socket, bound) = bind_socket(name, options)?;
        Ok(Self {
            socket,
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
            _bound: bound,
        })
    }

    pub(crate) fn accept(&self, timeout: Option<Duration>) -> Result<Stream, Error> {
        let deadline = timeout.map(|limit| Instant::now() + limit);
        loop {
            let left = deadline.map(|until| until.saturating_duration_since(Instant::now()));
            match wait_ready(self.socket.as_fd(), READABLE, left) {
                Ok(true) => {}
                Ok(false) => return Err(Error::TimedOut),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(Error::Io(error)),
            }
            match self.socket.accept() {
                Ok((socket, _)) => {
                    // Whether an accepted socket inherits the listener's mode differs by system.
                    socket.set_nonblocking(false)?;
                    return Ok(Stream::new(socket, self.read_timeout, self.write_timeout)?);
                }
                // A client gone since the wait, or another caller first: wait again.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionAborted
                    ) => {}
                Err(error) => return Err(Error::Io(error)),
            }
        }
    }
}
