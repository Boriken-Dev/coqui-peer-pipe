//! A connected stream, on either platform.
#![forbid(unsafe_code)]

use core::time::Duration;
use std::io::{self, Read, Write};

use coqui_peer::{ReadTimeout, WriteTimeout};

use crate::error::Error;
use crate::identity::OsIdentity;
use crate::name::PipeName;
use crate::options::ConnectOptions;
use crate::sys;

/// One connection over a pipe or a socket: bytes in order, both ways, between two processes.
///
/// `Read` and `Write` are implemented for `Stream` and `&Stream`, so one thread can read while another
/// writes. [`ReadTimeout`] and [`WriteTimeout`] make every `coqui_peer` driver work on it, deadlines
/// included. A read that times out loses nothing. Writing past a write timeout: on Unix a write reports
/// what it wrote. A write on Windows cannot say how much of it the peer already took, so one that
/// times out refuses every later write with `BrokenPipe`.
#[derive(Debug)]
pub struct Stream {
    inner: sys::Stream,
}

impl Stream {
    /// Connects to `name` with [`ConnectOptions`]'s defaults: at most 5 s to wait for room, and 30 s
    /// bounds on each read and write. On Windows the server can learn who this client is and cannot
    /// act as it.
    ///
    /// Nothing here proves who listens: another process may hold the name. Run a key proof over the
    /// stream ([`peer`](Self::peer) only says what the operating system reports).
    ///
    /// # Errors
    ///
    /// [`Error::NotListening`], [`Error::AccessDenied`], [`Error::TimedOut`], or [`Error::Io`].
    pub fn connect(name: &PipeName) -> Result<Self, Error> {
        Self::with_options(name, ConnectOptions::new())
    }

    /// Settings for [`connect`](Self::connect).
    pub fn options() -> ConnectOptions {
        ConnectOptions::new()
    }

    pub(crate) fn with_options(name: &PipeName, options: ConnectOptions) -> Result<Self, Error> {
        options.check()?;
        options.log_weakened();
        Ok(Self {
            inner: sys::Stream::connect(name, &options)?,
        })
    }

    pub(crate) fn from_sys(inner: sys::Stream) -> Self {
        Self { inner }
    }

    /// Who the operating system says is at the other end. Information, never a proof: see
    /// [`OsIdentity`].
    ///
    /// # Errors
    ///
    /// When the operating system does not answer at all; what it would not say is `None` in the result.
    pub fn peer(&self) -> io::Result<OsIdentity> {
        self.inner.peer()
    }

    /// Ends the connection both ways. What this side already wrote stays readable by the peer, which
    /// then sees the end. A read or a write blocked in another thread returns: the read with `Ok(0)`,
    /// the write with `BrokenPipe`, as do later ones. Dropping the stream does the same.
    ///
    /// # Errors
    ///
    /// When the operating system refuses.
    pub fn shutdown(&self) -> io::Result<()> {
        self.inner.shutdown()
    }

    /// Ends the writing direction only, where the system can: see [`WriteHalf`](crate::WriteHalf).
    #[cfg(feature = "channel")]
    pub(crate) fn shutdown_write(&self) -> io::Result<()> {
        self.inner.shutdown_write()
    }
}

impl Read for &Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for &Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadTimeout for Stream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_read_timeout(timeout)
    }
}

impl ReadTimeout for &Stream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_read_timeout(timeout)
    }
}

impl WriteTimeout for Stream {
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_write_timeout(timeout)
    }
}

impl WriteTimeout for &Stream {
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_write_timeout(timeout)
    }
}
