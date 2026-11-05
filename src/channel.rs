//! A `coqui-web` channel over a [`Stream`], so anything built on channels runs on a pipe.
#![forbid(unsafe_code)]

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::time::Duration;
use std::io::{self, Read, Write};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Instant;

use coqui_peer::{ReadTimeout, Weakening, WriteTimeout};
use coqui_web::Failure;

use crate::frame::{DEFAULT_MAX_MESSAGE, Decoder, encode, weakened};
use crate::identity::OsIdentity;
use crate::stream::Stream;

/// The default bound on bytes queued to send, as `coqui_web::Limits`.
pub const DEFAULT_QUEUED_BYTES: usize = 64 * 1024 * 1024;
/// The default time the peer may take nothing while there is something to send, as `coqui_web::Limits`.
pub const DEFAULT_SEND_LIMIT: Duration = Duration::from_secs(30);

/// Settings of a [`Channel`].
#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct ChannelOptions {
    max_message: usize,
    queued_bytes: usize,
    send_limit: Duration,
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            max_message: DEFAULT_MAX_MESSAGE,
            queued_bytes: DEFAULT_QUEUED_BYTES,
            send_limit: DEFAULT_SEND_LIMIT,
        }
    }
}

impl ChannelOptions {
    /// The defaults: 1 MiB messages, 64 MiB queued to send, 30 s for a peer that takes nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// The largest message, both ways, until a `set_max_` call changes it.
    pub fn max_message(mut self, bytes: usize) -> Self {
        self.max_message = bytes;
        self
    }

    /// The most bytes queued to send; a message that does not fit waits or is refused, as
    /// [`send`](coqui_web::Channel::send) and [`send_timeout`](coqui_web::Channel::send_timeout) say.
    pub fn queued_bytes(mut self, bytes: usize) -> Self {
        self.queued_bytes = bytes;
        self
    }

    /// How long the peer may take nothing while there is something to send, after which the channel
    /// ends with `Failure::NoProgress`. Also the most [`close`](coqui_web::Channel::close) waits for
    /// what is queued to go out.
    pub fn send_limit(mut self, limit: Duration) -> Self {
        self.send_limit = limit;
        self
    }

    /// The settings looser than the default.
    #[must_use]
    pub fn weakened(&self) -> Vec<Weakening> {
        let mut list = weakened(self.max_message);
        if self.queued_bytes > DEFAULT_QUEUED_BYTES {
            list.push(Weakening::new(
                "queued_bytes",
                self.queued_bytes.to_string(),
                DEFAULT_QUEUED_BYTES.to_string(),
            ));
        }
        if self.send_limit > DEFAULT_SEND_LIMIT {
            list.push(Weakening::new(
                "send_limit",
                alloc::format!("{:?}", self.send_limit),
                alloc::format!("{DEFAULT_SEND_LIMIT:?}"),
            ));
        }
        list
    }

    /// Starts a channel on `stream`, with a thread of its own that writes what is queued; the channel
    /// owns that thread and joins it when closed.
    ///
    /// # Errors
    ///
    /// When the thread cannot be started, or the stream refuses the timeout.
    pub fn start(self, stream: Stream) -> io::Result<Channel> {
        Channel::with_options(stream, self)
    }
}

#[derive(Debug, Default)]
struct Queue {
    frames: VecDeque<Vec<u8>>,
    /// Queued and not yet wholly written.
    bytes: usize,
    closing: bool,
    written_out: bool,
    ended: Option<Failure>,
}

#[derive(Debug)]
struct Reader {
    busy: bool,
    decoder: Decoder,
    ended: Option<Option<Failure>>,
}

#[derive(Debug)]
struct Bounds {
    receive: usize,
    send: usize,
    /// After this many more returned messages, the receive bound becomes this.
    staged: Option<(usize, usize)>,
}

#[derive(Debug)]
struct Shared {
    stream: Stream,
    send_limit: Duration,
    queued_limit: usize,
    queue: Mutex<Queue>,
    /// Something to write, or closing.
    work: Condvar,
    /// Room freed, or the channel ended.
    room: Condvar,
    /// The writer ended.
    finished: Condvar,
    reader: Mutex<Reader>,
    /// A reader finished, so the next may take its turn.
    turn: Condvar,
    bounds: Mutex<Bounds>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn wait_until<'a, T>(
    condition: &Condvar,
    guard: MutexGuard<'a, T>,
    deadline: Option<Instant>,
) -> MutexGuard<'a, T> {
    match deadline {
        None => condition
            .wait(guard)
            .unwrap_or_else(PoisonError::into_inner),
        Some(until) => {
            let left = until.saturating_duration_since(Instant::now());
            condition
                .wait_timeout(guard, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0
        }
    }
}

/// A [`coqui_web::Channel`] over a [`Stream`]: every message one frame, as [`Messages`](crate::Messages).
///
/// It is not authenticated: run a key proof on it before anything else. A send goes on a queue that its
/// writer thread empties, so a message once queued is not taken back, and one refused for want of room
/// was not sent: a timed-out send leaves the stream whole on every platform. Reading happens in
/// `receive` itself, so nothing is read ahead of the reader and a peer's sends are held back by a slow
/// reader.
///
/// A message above the receive bound ends the channel with `Failure::Protocol`, and the peer sees the
/// connection end. A peer that takes nothing for the send limit ends it with `Failure::NoProgress`.
/// `close`, which dropping also does, sends what is queued, for at most the send limit, then ends the
/// connection and joins the writer.
#[derive(Debug)]
pub struct Channel {
    shared: Arc<Shared>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl Channel {
    /// A channel on `stream` with [`ChannelOptions`]'s defaults.
    ///
    /// # Errors
    ///
    /// As [`ChannelOptions::start`].
    pub fn start(stream: Stream) -> io::Result<Self> {
        Self::with_options(stream, ChannelOptions::new())
    }

    /// Settings for a channel.
    pub fn options() -> ChannelOptions {
        ChannelOptions::new()
    }

    fn with_options(stream: Stream, options: ChannelOptions) -> io::Result<Self> {
        stream.set_write_timeout(Some(options.send_limit))?;
        let shared = Arc::new(Shared {
            stream,
            send_limit: options.send_limit,
            queued_limit: options.queued_bytes,
            queue: Mutex::default(),
            work: Condvar::new(),
            room: Condvar::new(),
            finished: Condvar::new(),
            reader: Mutex::new(Reader {
                busy: false,
                decoder: Decoder::default(),
                ended: None,
            }),
            turn: Condvar::new(),
            bounds: Mutex::new(Bounds {
                receive: options.max_message,
                send: options.max_message,
                staged: None,
            }),
        });
        let writing = Arc::clone(&shared);
        let writer = std::thread::Builder::new()
            .name("coqui-peer-pipe writer".into())
            .spawn(move || writing.write_queue())?;
        Ok(Self {
            shared,
            writer: Mutex::new(Some(writer)),
        })
    }

    /// Who the operating system says is at the other end: see [`Stream::peer`].
    ///
    /// # Errors
    ///
    /// As [`Stream::peer`].
    pub fn peer(&self) -> io::Result<OsIdentity> {
        self.shared.stream.peer()
    }
}

impl Shared {
    fn write_queue(&self) {
        loop {
            let frame = {
                let mut queue = locked(&self.queue);
                loop {
                    if let Some(frame) = queue.frames.pop_front() {
                        break frame;
                    }
                    if queue.closing {
                        queue.written_out = true;
                        self.finished.notify_all();
                        return;
                    }
                    queue = self
                        .work
                        .wait(queue)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };
            let mut stream = &self.stream;
            if let Err(error) = stream.write_all(&frame) {
                self.end_writing(if error.kind() == io::ErrorKind::TimedOut {
                    Failure::NoProgress
                } else {
                    Failure::PathLost
                });
                return;
            }
            locked(&self.queue).bytes -= frame.len();
            self.room.notify_all();
        }
    }

    fn end_writing(&self, failure: Failure) {
        let mut queue = locked(&self.queue);
        queue.ended = Some(failure);
        queue.frames.clear();
        queue.bytes = 0;
        queue.written_out = true;
        drop(queue);
        let _ = self.stream.shutdown();
        self.room.notify_all();
        self.finished.notify_all();
    }

    /// Queues one frame if it fits, saying so.
    fn try_queue(&self, queue: &mut Queue, frame: &mut Option<Vec<u8>>) -> Result<bool, Failure> {
        if let Some(failure) = queue.ended {
            return Err(failure);
        }
        if queue.closing {
            return Err(Failure::PathLost);
        }
        let length = frame.as_ref().map_or(0, Vec::len);
        if queue.bytes + length > self.queued_limit {
            return Ok(false);
        }
        if let Some(frame) = frame.take() {
            queue.bytes += length;
            queue.frames.push_back(frame);
            self.work.notify_one();
        }
        Ok(true)
    }

    fn frame(&self, message: &[u8]) -> Result<Vec<u8>, Failure> {
        let bound = locked(&self.bounds).send;
        encode(message, bound).map_err(|_| Failure::Protocol)
    }

    fn receive_bound(&self) -> usize {
        locked(&self.bounds).receive
    }

    /// One message has gone to the reader, which moves a staged bound on.
    fn taken(&self) {
        let mut bounds = locked(&self.bounds);
        if let Some((left, bytes)) = bounds.staged {
            if left <= 1 {
                bounds.receive = bytes;
                bounds.staged = None;
            } else {
                bounds.staged = Some((left - 1, bytes));
            }
        }
    }

    /// Reads one message into `decoder`.
    fn read_message(
        &self,
        decoder: &mut Decoder,
        deadline: Option<Instant>,
    ) -> Result<Option<Vec<u8>>, Failure> {
        let mut stream = &self.stream;
        loop {
            // A zero timeout is no bound to a stream, so a spent wait still looks once.
            let left = deadline.map(|until| {
                until
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1))
            });
            stream
                .set_read_timeout(left)
                .map_err(|_| Failure::PathLost)?;
            let count = match stream.read(decoder.space()) {
                Ok(0) if decoder.is_idle() => return Ok(None),
                Ok(0) => return Err(Failure::PathLost),
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                    return Err(Failure::Timeout);
                }
                Err(_) => return Err(Failure::PathLost),
            };
            match decoder.filled(count, self.receive_bound()) {
                Ok(Some(message)) => return Ok(Some(message)),
                Ok(None) => {}
                Err(_) => return Err(Failure::Protocol),
            }
        }
    }
}

impl coqui_web::Channel for Channel {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        let mut frame = Some(self.shared.frame(message)?);
        let mut queue = locked(&self.shared.queue);
        if self.shared.try_queue(&mut queue, &mut frame)? {
            Ok(())
        } else {
            Err(Failure::NoProgress)
        }
    }

    fn send_timeout(&self, message: &[u8], timeout: Option<Duration>) -> Result<(), Failure> {
        let mut frame = Some(self.shared.frame(message)?);
        // Room for more than the whole budget never comes.
        if frame.as_ref().map_or(0, Vec::len) > self.shared.queued_limit {
            return Err(Failure::NoProgress);
        }
        let deadline = timeout.map(|limit| Instant::now() + limit);
        let mut queue = locked(&self.shared.queue);
        loop {
            if self.shared.try_queue(&mut queue, &mut frame)? {
                return Ok(());
            }
            if deadline.is_some_and(|until| Instant::now() >= until) {
                return Err(Failure::Timeout);
            }
            queue = wait_until(&self.shared.room, queue, deadline);
        }
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        let deadline = timeout.map(|limit| Instant::now() + limit);
        let mut reader = locked(&self.shared.reader);
        while reader.busy {
            if deadline.is_some_and(|until| Instant::now() >= until) {
                return Err(Failure::Timeout);
            }
            reader = wait_until(&self.shared.turn, reader, deadline);
        }
        if let Some(ended) = reader.ended {
            return match ended {
                None => Ok(None),
                Some(failure) => Err(failure),
            };
        }
        reader.busy = true;
        let mut decoder = core::mem::take(&mut reader.decoder);
        drop(reader);

        let outcome = self.shared.read_message(&mut decoder, deadline);

        let mut reader = locked(&self.shared.reader);
        reader.busy = false;
        reader.decoder = decoder;
        match outcome {
            Ok(Some(_)) => self.shared.taken(),
            Ok(None) => reader.ended = Some(None),
            Err(Failure::Timeout) => {}
            Err(failure) => {
                reader.ended = Some(Some(failure));
                // What cannot be read past is the end of this connection, for the peer as well.
                let _ = self.shared.stream.shutdown();
            }
        }
        self.shared.turn.notify_one();
        outcome
    }

    fn close(&self) {
        let writer = locked(&self.writer).take();
        let mut queue = locked(&self.shared.queue);
        queue.closing = true;
        self.shared.work.notify_all();
        let until = Instant::now() + self.shared.send_limit;
        while !queue.written_out && Instant::now() < until {
            queue = wait_until(&self.shared.finished, queue, Some(until));
        }
        drop(queue);
        // Also wakes a writer still blocked, a reader, and whoever waits for room.
        let _ = self.shared.stream.shutdown();
        self.shared.room.notify_all();
        if let Some(writer) = writer {
            let _ = writer.join();
        }
    }

    fn set_max_message(&self, bytes: usize) {
        let mut bounds = locked(&self.shared.bounds);
        bounds.receive = bytes;
        bounds.send = bytes;
        bounds.staged = None;
    }

    fn set_max_receive(&self, bytes: usize) {
        let mut bounds = locked(&self.shared.bounds);
        bounds.receive = bytes;
        bounds.staged = None;
    }

    fn set_max_receive_after(&self, messages: usize, bytes: usize) -> Result<(), Failure> {
        let mut bounds = locked(&self.shared.bounds);
        if messages == 0 {
            bounds.receive = bytes;
            bounds.staged = None;
        } else {
            bounds.staged = Some((messages, bytes));
        }
        Ok(())
    }

    fn set_max_send(&self, bytes: usize) {
        locked(&self.shared.bounds).send = bytes;
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        coqui_web::Channel::close(self);
    }
}
