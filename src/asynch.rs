//! The listener, the stream and the message transport over tokio.
//!
//! Binding and connecting run the same checks as the blocking forms. These need a tokio runtime with
//! its I/O driver enabled: tokio panics otherwise. Streams have no timeouts of their own: wrap a call in
//! `tokio::time::timeout`, or use `coqui_peer::asynch`'s `_until` drivers.
#![forbid(unsafe_code)]

use alloc::vec::Vec;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::time::Instant;

use coqui_peer::{Weakening, asynch::MessageTransport};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::error::Error;
use crate::frame::{DEFAULT_MAX_MESSAGE, Decoder, encode, too_long, torn, weakened};
use crate::identity::OsIdentity;
use crate::name::PipeName;
use crate::options::{ConnectOptions, ListenerOptions};
use crate::sys;

/// How often a wait for a free instance or a place in the queue looks again.
const BUSY_POLL: core::time::Duration = core::time::Duration::from_millis(10);

#[cfg(windows)]
use tokio::net::windows::named_pipe::{NamedPipeClient, NamedPipeServer};

#[cfg(unix)]
#[derive(Debug)]
struct Waiting {
    socket: tokio::net::UnixListener,
    _bound: sys::Bound,
}

#[cfg(windows)]
#[derive(Debug)]
struct Waiting {
    instances: sys::Instances,
    pending: NamedPipeServer,
    spare: Option<NamedPipeServer>,
}

/// [`Listener`](crate::Listener) over tokio, with the same access control. Dropping it ends listening.
#[derive(Debug)]
pub struct Listener {
    waiting: Waiting,
    name: PipeName,
    weakened: Vec<Weakening>,
}

impl Listener {
    /// Binds `name` with [`ListenerOptions`]'s strict defaults.
    ///
    /// # Errors
    ///
    /// As [`Listener::bind`](crate::Listener::bind).
    pub fn bind(name: &PipeName) -> Result<Self, Error> {
        Self::bind_with(name, ListenerOptions::new())
    }

    /// Binds `name` with `options`. Its timeouts belong to blocking streams and mean nothing here.
    ///
    /// # Errors
    ///
    /// As [`ListenerOptions::bind`].
    pub fn bind_with(name: &PipeName, options: ListenerOptions) -> Result<Self, Error> {
        options.check()?;
        options.log_weakened();
        let weakened = options.weakened();
        Ok(Self {
            waiting: Waiting::bind(name, &options)?,
            name: name.clone(),
            weakened,
        })
    }

    /// Waits for the next connection. Cancelling this loses nothing.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the operating system fails.
    pub async fn accept(&mut self) -> Result<Stream, Error> {
        self.waiting.accept().await
    }

    /// The name this listens on.
    #[must_use]
    pub fn name(&self) -> &PipeName {
        &self.name
    }

    /// Every setting this was bound with that is looser than its default.
    #[must_use]
    pub fn weakened(&self) -> &[Weakening] {
        &self.weakened
    }
}

#[cfg(unix)]
impl Waiting {
    fn bind(name: &PipeName, options: &ListenerOptions) -> Result<Self, Error> {
        let (socket, bound) = sys::bind_socket(name, options)?;
        Ok(Self {
            socket: tokio::net::UnixListener::from_std(socket)?,
            _bound: bound,
        })
    }

    async fn accept(&mut self) -> Result<Stream, Error> {
        let (socket, _) = self.socket.accept().await?;
        Ok(Stream {
            inner: Inner::Socket(socket),
        })
    }
}

#[cfg(windows)]
impl Waiting {
    fn bind(name: &PipeName, options: &ListenerOptions) -> Result<Self, Error> {
        let instances = sys::Instances::new(name, options)?;
        let pending = instances.create(true)?.ok_or(Error::NameTaken)?;
        Ok(Self {
            instances,
            pending: sys::server_from(pending)?,
            spare: None,
        })
    }

    async fn accept(&mut self) -> Result<Stream, Error> {
        while self.spare.is_none() {
            match self.instances.create(false)? {
                Some(instance) => self.spare = Some(sys::server_from(instance)?),
                None => tokio::time::sleep(BUSY_POLL).await,
            }
        }
        if let Err(error) = self.pending.connect().await
            && !sys::is_client_gone(&error)
        {
            return Err(Error::Io(error));
        }
        let next = self
            .spare
            .take()
            .expect("the loop above leaves a spare instance");
        let connected = core::mem::replace(&mut self.pending, next);
        Ok(Stream {
            inner: Inner::Server(connected),
        })
    }
}

#[derive(Debug)]
enum Inner {
    #[cfg(unix)]
    Socket(tokio::net::UnixStream),
    #[cfg(windows)]
    Server(NamedPipeServer),
    #[cfg(windows)]
    Client(NamedPipeClient),
}

/// [`Stream`](crate::Stream) over tokio: `AsyncRead + AsyncWrite`, so every driver of
/// `coqui_peer::asynch` works on it.
///
/// Dropping it ends the connection, with what was written still readable by the peer.
/// `poll_shutdown` ends the writing direction on Unix; on Windows a pipe has no half-close and it
/// does nothing.
#[derive(Debug)]
pub struct Stream {
    inner: Inner,
}

impl Stream {
    /// Connects to `name` with [`ConnectOptions`]'s defaults.
    ///
    /// # Errors
    ///
    /// As [`Stream::connect`](crate::Stream::connect).
    pub async fn connect(name: &PipeName) -> Result<Self, Error> {
        Self::connect_with(name, ConnectOptions::new()).await
    }

    /// Connects to `name` with `options`. Its read and write timeouts belong to blocking streams and
    /// mean nothing here.
    ///
    /// # Errors
    ///
    /// As [`ConnectOptions::connect`].
    pub async fn connect_with(name: &PipeName, options: ConnectOptions) -> Result<Self, Error> {
        options.check()?;
        options.log_weakened();
        let deadline = Instant::now() + options.timeout;
        loop {
            if let Some(inner) = Inner::try_connect(name, &options, deadline)? {
                return Ok(Self { inner });
            }
            if Instant::now() >= deadline {
                return Err(Error::TimedOut);
            }
            tokio::time::sleep(BUSY_POLL).await;
        }
    }

    /// Who the operating system says is at the other end: see [`OsIdentity`].
    ///
    /// # Errors
    ///
    /// When the operating system does not answer at all.
    pub fn peer(&self) -> io::Result<OsIdentity> {
        match &self.inner {
            #[cfg(unix)]
            Inner::Socket(socket) => sys::peer_of(socket),
            #[cfg(windows)]
            Inner::Server(pipe) => Ok(sys::peer_of_tokio(pipe, true)),
            #[cfg(windows)]
            Inner::Client(pipe) => Ok(sys::peer_of_tokio(pipe, false)),
        }
    }
}

impl Inner {
    #[cfg(unix)]
    fn try_connect(
        name: &PipeName,
        _options: &ConnectOptions,
        deadline: Instant,
    ) -> Result<Option<Self>, Error> {
        match sys::try_connect(name.path(), deadline)? {
            Some(socket) => {
                socket.set_nonblocking(true)?;
                Ok(Some(Self::Socket(tokio::net::UnixStream::from_std(
                    socket,
                )?)))
            }
            None => Ok(None),
        }
    }

    #[cfg(windows)]
    fn try_connect(
        name: &PipeName,
        options: &ConnectOptions,
        _deadline: Instant,
    ) -> Result<Option<Self>, Error> {
        let path = sys::path_of(name)?;
        match sys::open(&path, options.impersonation)? {
            Some(handle) => Ok(Some(Self::Client(sys::client_from(handle)?))),
            None => Ok(None),
        }
    }
}

macro_rules! dispatch {
    ($self:ident, $inner:ident => $call:expr) => {
        match &mut $self.get_mut().inner {
            #[cfg(unix)]
            Inner::Socket($inner) => $call,
            #[cfg(windows)]
            Inner::Server($inner) => $call,
            #[cfg(windows)]
            Inner::Client($inner) => $call,
        }
    };
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        dispatch!(self, inner => Pin::new(inner).poll_read(context, buffer))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        dispatch!(self, inner => Pin::new(inner).poll_write(context, buffer))
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        dispatch!(self, inner => Pin::new(inner).poll_flush(context))
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        dispatch!(self, inner => Pin::new(inner).poll_shutdown(context))
    }
}

/// [`Messages`](crate::Messages) over a tokio stream, as a [`MessageTransport`] of
/// `coqui_peer::asynch`.
///
/// A cancelled receive keeps what arrived for the next one. A send that failed or was cancelled after
/// it began refuses every later send, since the next frame could not be found.
#[derive(Debug)]
pub struct Messages<S> {
    io: S,
    decoder: Decoder,
    max_message: usize,
    sending: bool,
    broken: bool,
}

impl<S> Messages<S> {
    /// Messages over `io`, each at most [`DEFAULT_MAX_MESSAGE`](crate::DEFAULT_MAX_MESSAGE) bytes.
    #[must_use]
    pub fn new(io: S) -> Self {
        Self {
            io,
            decoder: Decoder::default(),
            max_message: DEFAULT_MAX_MESSAGE,
            sending: false,
            broken: false,
        }
    }

    /// The largest message this sends and receives. It can be above the default, which
    /// [`weakened`](Self::weakened) then lists.
    #[must_use]
    pub fn with_max_message(mut self, bytes: usize) -> Self {
        self.max_message = bytes;
        self
    }

    /// The largest message this sends and receives.
    #[must_use]
    pub fn max_message(&self) -> usize {
        self.max_message
    }

    /// The settings looser than the default.
    #[must_use]
    pub fn weakened(&self) -> Vec<Weakening> {
        weakened(self.max_message)
    }

    /// The stream.
    #[must_use]
    pub fn get_ref(&self) -> &S {
        &self.io
    }

    /// The stream, with any part of a frame already read dropped.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.io
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> MessageTransport for Messages<S> {
    async fn send(&mut self, message: &[u8]) -> io::Result<()> {
        if self.sending {
            return Err(torn());
        }
        let frame = encode(message, self.max_message)?;
        self.sending = true;
        self.io.write_all(&frame).await?;
        self.io.flush().await?;
        self.sending = false;
        Ok(())
    }

    async fn recv(&mut self) -> io::Result<Vec<u8>> {
        if self.broken {
            return Err(too_long());
        }
        loop {
            let count = self.io.read(self.decoder.space()).await?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            match self.decoder.filled(count, self.max_message) {
                Ok(Some(message)) => return Ok(message),
                Ok(None) => {}
                Err(error) => {
                    self.broken = true;
                    return Err(error);
                }
            }
        }
    }
}
