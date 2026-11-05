//! Unix domain sockets. Binding, access control and waiting, in safe code.
#![forbid(unsafe_code)]

use core::time::Duration;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::fd::AsFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Instant;

use socket2::{Domain, SockAddr, Socket, Type};

use super::unix_os::{READABLE, WRITABLE, effective_user, peer_credentials, wait_ready};
use crate::error::Error;
use crate::identity::OsIdentity;
use crate::name::PipeName;
use crate::options::{ConnectOptions, ListenerOptions};

/// How often a connect looks again at a queue with no room.
const FULL_QUEUE_POLL: Duration = Duration::from_millis(5);

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
        (&self.socket).write(buffer).map_err(one_timeout)
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
        shutdown(&self.socket)
    }
}

/// The socket's end in both directions; one the peer already ended is ended.
fn shutdown(socket: &UnixStream) -> io::Result<()> {
    match socket.shutdown(Shutdown::Both) {
        Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
        other => other,
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

/// The socket's path, removed when the listener ends if it is still this socket.
#[derive(Debug)]
pub(crate) struct Bound {
    path: PathBuf,
}

impl Drop for Bound {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Binds `name`, returning a blocking-free listener and what removes its path.
pub(crate) fn bind_socket(
    name: &PipeName,
    options: &ListenerOptions,
) -> Result<(UnixListener, Bound), Error> {
    let address = SockAddr::unix(name.path()).map_err(|_| Error::InvalidName)?;
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.bind(&address).map_err(|error| match error.kind() {
        io::ErrorKind::AddrInUse => Error::NameTaken,
        _ => Error::Io(error),
    })?;
    socket.listen(options.backlog)?;
    socket.set_nonblocking(true)?;
    Ok((
        socket.into(),
        Bound {
            path: name.path().to_path_buf(),
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
                    // A BSD socket inherits the listener's non-blocking mode.
                    socket.set_nonblocking(false)?;
                    return Ok(Stream::new(socket, self.read_timeout, self.write_timeout)?);
                }
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
