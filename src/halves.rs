//! A [`Stream`] as a [`Split`], so a `coqui_transport::StreamChannel` runs on a pipe or a socket.
#![forbid(unsafe_code)]

use alloc::sync::Arc;
use std::io::{self, Read, Write};

use coqui_transport::{Halves, Interrupt, Split};

use crate::stream::Stream;

/// The reading half of a split [`Stream`].
#[derive(Debug)]
pub struct ReadHalf {
    stream: Arc<Stream>,
}

/// The writing half of a split [`Stream`]. Dropping it ends the writing direction: the peer reads what
/// was written, then the end. On Windows a pipe has no half-close, so it ends both directions.
#[derive(Debug)]
pub struct WriteHalf {
    stream: Arc<Stream>,
}

impl Read for ReadHalf {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut stream = &*self.stream;
        stream.read(buffer)
    }
}

impl Write for WriteHalf {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let mut stream = &*self.stream;
        stream.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        let _ = self.stream.shutdown_write();
    }
}

/// The halves share the stream's own timeouts, and the interrupt is [`Stream::shutdown`].
impl Split for Stream {
    type Reader = ReadHalf;
    type Writer = WriteHalf;

    fn split(self) -> io::Result<Halves<ReadHalf, WriteHalf>> {
        let stream = Arc::new(self);
        let ending = Arc::clone(&stream);
        let interrupt = Interrupt::new(move || {
            let _ = ending.shutdown();
        });
        let reader = ReadHalf {
            stream: Arc::clone(&stream),
        };
        Ok(Halves::new(reader, WriteHalf { stream }).with_interrupt(interrupt))
    }
}
