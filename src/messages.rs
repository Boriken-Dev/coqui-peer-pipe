//! [`Messages`]: `coqui_peer`'s message transport over a stream, framed by `coqui_transport`'s
//! [`PeerFrame`].
//!
//! The decoder does no I/O, so the blocking and the async transport share it, and a read that times
//! out or is cancelled loses nothing: what arrived stays in it for the next call.
#![forbid(unsafe_code)]

use alloc::vec::Vec;
use core::cell::Cell;
use core::time::Duration;
use std::io::{self, Read, Write};
use std::time::Instant;

use coqui_peer::{MessageTransport, ReadTimeout, Weakening, WriteTimeout};
use coqui_transport::frame::{Codec, PeerDecoder, PeerFrame};

pub use coqui_transport::frame::DEFAULT_MAX_MESSAGE;

use crate::deadline;

/// The most one read takes from the stream, so a frame's declared length costs nothing until it is sent.
pub(crate) const READ_CHUNK: usize = 16 * 1024;

pub(crate) fn too_long() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a frame is above the message bound",
    )
}

/// One frame as one buffer, so it reaches the stream in one write where the stream allows it.
pub(crate) fn encode(message: &[u8], max: usize) -> io::Result<Vec<u8>> {
    PeerFrame
        .encode(message, max)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a message is above the bound"))
}

pub(crate) fn torn() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "an earlier send ended inside a frame",
    )
}

pub(crate) fn weakened(max: usize) -> Vec<Weakening> {
    if max > DEFAULT_MAX_MESSAGE {
        alloc::vec![Weakening::new(
            "max_message",
            max.to_string(),
            DEFAULT_MAX_MESSAGE.to_string(),
        )]
    } else {
        Vec::new()
    }
}

/// A [`MessageTransport`] over a byte stream: every message is one frame, bounded in size.
///
/// A receive that times out keeps what arrived and the next one goes on from there. A declared length
/// above the bound is refused before anything of it is read, and the stream is then unusable, since
/// the next frame cannot be found. A send that stopped inside a frame refuses every later send, for
/// the same reason. Over a [`Stream`](crate::Stream) on Windows a timed-out send refuses later sends in
/// any case: see [`WriteTimeout`] there.
///
/// A receive reads ahead of the frame it returns, so a frame that arrived behind it waits here for the
/// next receive.
#[derive(Debug)]
pub struct Messages<S> {
    io: S,
    decoder: PeerDecoder,
    max_message: usize,
    torn: bool,
    /// Set through [`ReadTimeout`] on `&self`: a bound on a whole receive, not on each read of it.
    read_limit: Cell<Option<Duration>>,
    /// Set through [`WriteTimeout`] on `&self`: a bound on a whole send.
    write_limit: Cell<Option<Duration>>,
}

impl<S> Messages<S> {
    /// Messages over `io`, each at most [`DEFAULT_MAX_MESSAGE`] bytes.
    #[must_use]
    pub fn new(io: S) -> Self {
        Self {
            io,
            decoder: PeerDecoder::new(DEFAULT_MAX_MESSAGE),
            max_message: DEFAULT_MAX_MESSAGE,
            torn: false,
            read_limit: Cell::new(None),
            write_limit: Cell::new(None),
        }
    }

    /// The largest message this sends and receives. It can be above the default, which
    /// [`weakened`](Self::weakened) then lists; a frame cannot hold more than `u32::MAX` bytes.
    #[must_use]
    pub fn with_max_message(mut self, bytes: usize) -> Self {
        self.max_message = bytes;
        self.decoder.set_maximum(bytes);
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

    /// The stream, with any frame or part of one already read dropped.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.io
    }
}

/// What is left until `deadline`, or `TimedOut` once nothing is.
fn left_until(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| io::ErrorKind::TimedOut.into())
}

impl<S: Read + Write + ReadTimeout + WriteTimeout> MessageTransport for Messages<S> {
    /// # Errors
    ///
    /// `InvalidInput` for a message above the bound, with nothing written. Otherwise the stream's
    /// error; `BrokenPipe` once an earlier send stopped inside a frame.
    fn send(&mut self, message: &[u8]) -> io::Result<()> {
        if self.torn {
            return Err(torn());
        }
        let frame = encode(message, self.max_message)?;
        let deadline = deadline::within(self.write_limit.get());
        let mut written = 0;
        while written < frame.len() {
            if let Some(until) = deadline {
                match left_until(until).and_then(|left| self.io.set_write_timeout(Some(left))) {
                    Ok(()) => {}
                    Err(error) => {
                        self.torn = written > 0;
                        return Err(error);
                    }
                }
            }
            match self.io.write(&frame[written..]) {
                Ok(0) => {
                    self.torn = written > 0;
                    return Err(io::ErrorKind::WriteZero.into());
                }
                Ok(count) => written += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.torn = written > 0;
                    return Err(error);
                }
            }
        }
        self.io.flush()
    }

    /// # Errors
    ///
    /// `InvalidData` for a declared length above the bound, and for every later call;
    /// `UnexpectedEof` when the stream ends, inside a frame or between frames; otherwise the stream's
    /// error, after which a timed-out call can be repeated.
    fn recv(&mut self) -> io::Result<Vec<u8>> {
        let deadline = deadline::within(self.read_limit.get());
        let mut chunk = [0_u8; READ_CHUNK];
        loop {
            match self.decoder.next_frame() {
                Ok(Some(message)) => return Ok(message),
                Ok(None) => {}
                Err(_) => return Err(too_long()),
            }
            if let Some(until) = deadline {
                self.io.set_read_timeout(Some(left_until(until)?))?;
            }
            match self.io.read(&mut chunk) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => self.decoder.push(&chunk[..count]),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl<S: ReadTimeout> ReadTimeout for Messages<S> {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.io.set_read_timeout(timeout)?;
        self.read_limit.set(timeout);
        Ok(())
    }
}

impl<S: WriteTimeout> WriteTimeout for Messages<S> {
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.io.set_write_timeout(timeout)?;
        self.write_limit.set(timeout);
        Ok(())
    }
}
