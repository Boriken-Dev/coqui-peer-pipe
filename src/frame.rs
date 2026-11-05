//! Length framing: four big-endian bytes, then the message, as `coqui_peer::write_frame`.
//!
//! The decoder does no I/O, so the blocking and the async transport share it, and a read that times
//! out or is cancelled loses nothing: what arrived stays here for the next call.
#![forbid(unsafe_code)]

use alloc::vec::Vec;
use core::cell::Cell;
use core::time::Duration;
use std::io::{self, Read, Write};
use std::time::Instant;

use coqui_peer::{MAX_MESSAGE, MessageTransport, ReadTimeout, Weakening, WriteTimeout};

/// How far the buffer grows ahead of what has arrived, so a declared length costs nothing until it
/// is sent.
const CHUNK: usize = 64 * 1024;
const HEADER: usize = 4;

/// The default largest message of [`Messages`], which is `coqui_peer::MAX_MESSAGE`.
pub const DEFAULT_MAX_MESSAGE: usize = MAX_MESSAGE;

/// Reads frames from bytes as they arrive.
#[derive(Debug, Default)]
pub(crate) struct Decoder {
    header: [u8; HEADER],
    filled: usize,
    body: Vec<u8>,
    length: Option<usize>,
}

impl Decoder {
    /// Whether no part of a frame has arrived, so a stream ending here ended in order.
    #[cfg(feature = "channel")]
    pub(crate) fn is_idle(&self) -> bool {
        self.length.is_none() && self.filled == 0
    }

    /// Where the next bytes go, never past this frame's end.
    pub(crate) fn space(&mut self) -> &mut [u8] {
        match self.length {
            None => &mut self.header[self.filled..],
            Some(length) => {
                let grown = length.min(self.filled + CHUNK);
                if self.body.len() < grown {
                    self.body.resize(grown, 0);
                }
                &mut self.body[self.filled..grown]
            }
        }
    }

    /// Takes `count` bytes written into [`space`](Self::space) and returns the message they complete.
    ///
    /// # Errors
    ///
    /// `InvalidData` when a header declares more than `max`, before any of it is read. The stream
    /// cannot find the next frame after that.
    pub(crate) fn filled(&mut self, count: usize, max: usize) -> io::Result<Option<Vec<u8>>> {
        self.filled += count;
        if self.length.is_none() {
            if self.filled < HEADER {
                return Ok(None);
            }
            let declared = u32::from_be_bytes(self.header);
            let length = usize::try_from(declared).unwrap_or(usize::MAX);
            if length > max {
                return Err(too_long());
            }
            self.length = Some(length);
            self.filled = 0;
        }
        if self.length == Some(self.filled) {
            let mut message = core::mem::take(&mut self.body);
            message.truncate(self.filled);
            self.length = None;
            self.filled = 0;
            return Ok(Some(message));
        }
        Ok(None)
    }
}

pub(crate) fn too_long() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a frame is above the message bound",
    )
}

/// One frame as one buffer, so it reaches the stream in one write where the stream allows it.
pub(crate) fn encode(message: &[u8], max: usize) -> io::Result<Vec<u8>> {
    let length = u32::try_from(message.len())
        .ok()
        .filter(|_| message.len() <= max)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "a message is above the bound")
        })?;
    let mut frame = Vec::with_capacity(HEADER + message.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(message);
    Ok(frame)
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
#[derive(Debug)]
pub struct Messages<S> {
    io: S,
    decoder: Decoder,
    max_message: usize,
    torn: bool,
    broken: bool,
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
            decoder: Decoder::default(),
            max_message: DEFAULT_MAX_MESSAGE,
            torn: false,
            broken: false,
            read_limit: Cell::new(None),
            write_limit: Cell::new(None),
        }
    }

    /// The largest message this sends and receives. It can be above the default, which
    /// [`weakened`](Self::weakened) then lists; a frame cannot hold more than `u32::MAX` bytes.
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
        let deadline = self.write_limit.get().map(|limit| Instant::now() + limit);
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
        if self.broken {
            return Err(too_long());
        }
        let deadline = self.read_limit.get().map(|limit| Instant::now() + limit);
        loop {
            if let Some(until) = deadline {
                self.io.set_read_timeout(Some(left_until(until)?))?;
            }
            let count = match self.io.read(self.decoder.space()) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
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
