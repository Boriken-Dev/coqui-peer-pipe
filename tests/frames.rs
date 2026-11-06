//! Length framing over a stream: the format, its bound, and a call that stops halfway.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::time::Duration;

use coqui_peer::{read_frame, write_frame};
use coqui_transport_pipe::{
    DEFAULT_MAX_MESSAGE, MessageTransport, Messages, ReadTimeout, WriteTimeout,
};
use proptest::prelude::*;

/// A scripted stream: bytes to read in the given chunks, an error between chunks, and what was written.
#[derive(Default)]
struct Script {
    chunks: VecDeque<Result<Vec<u8>, io::ErrorKind>>,
    written: Vec<u8>,
    /// Bytes a write takes before it fails with `TimedOut`.
    write_room: Option<usize>,
    reads: usize,
}

impl Read for Script {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reads += 1;
        match self.chunks.pop_front() {
            None => Ok(0),
            Some(Err(kind)) => Err(kind.into()),
            Some(Ok(mut chunk)) => {
                let count = chunk.len().min(buffer.len());
                buffer[..count].copy_from_slice(&chunk[..count]);
                if count < chunk.len() {
                    self.chunks.push_front(Ok(chunk.split_off(count)));
                }
                Ok(count)
            }
        }
    }
}

impl Write for Script {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let count = match self.write_room {
            Some(0) => return Err(io::ErrorKind::TimedOut.into()),
            Some(room) => room.min(buffer.len()),
            None => buffer.len(),
        };
        if let Some(room) = &mut self.write_room {
            *room -= count;
        }
        self.written.extend_from_slice(&buffer[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ReadTimeout for Script {
    fn set_read_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

impl WriteTimeout for Script {
    fn set_write_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

fn reading(chunks: &[&[u8]]) -> Messages<Script> {
    Messages::new(Script {
        chunks: chunks.iter().map(|chunk| Ok(chunk.to_vec())).collect(),
        ..Script::default()
    })
}

#[test]
fn a_frame_is_four_big_endian_length_bytes_and_the_message() {
    let mut messages = Messages::new(Script::default());
    messages.send(b"hello").unwrap();
    assert_eq!(messages.get_ref().written, b"\x00\x00\x00\x05hello");
}

#[test]
fn the_format_is_coqui_peers_both_ways() {
    let mut messages = Messages::new(Script::default());
    messages.send(b"to coqui-peer").unwrap();
    let mut sent = messages.get_ref().written.as_slice();
    assert_eq!(read_frame(&mut sent).unwrap(), b"to coqui-peer");

    let mut framed = Vec::new();
    write_frame(&mut framed, b"from coqui-peer").unwrap();
    let mut messages = reading(&[&framed]);
    assert_eq!(messages.recv().unwrap(), b"from coqui-peer");
}

#[test]
fn an_empty_message_is_a_message() {
    let mut messages = reading(&[b"\x00\x00\x00\x00", b"\x00\x00\x00\x01x"]);
    assert_eq!(messages.recv().unwrap(), b"");
    assert_eq!(messages.recv().unwrap(), b"x");
}

#[test]
fn a_frame_may_arrive_one_byte_at_a_time() {
    let framed = b"\x00\x00\x00\x03abc";
    let chunks: Vec<&[u8]> = framed.chunks(1).collect();
    assert_eq!(reading(&chunks).recv().unwrap(), b"abc");
}

#[test]
fn a_receive_that_times_out_keeps_what_arrived() {
    for split in 1..7 {
        let framed = b"\x00\x00\x00\x03abc";
        let (first, second) = framed.split_at(split);
        let mut messages = Messages::new(Script {
            chunks: VecDeque::from([
                Ok(first.to_vec()),
                Err(io::ErrorKind::TimedOut),
                Ok(second.to_vec()),
            ]),
            ..Script::default()
        });
        assert_eq!(
            messages.recv().unwrap_err().kind(),
            io::ErrorKind::TimedOut,
            "split at {split}"
        );
        assert_eq!(messages.recv().unwrap(), b"abc", "split at {split}");
    }
}

#[test]
fn a_declared_length_above_the_bound_is_refused_before_it_is_read() {
    let mut messages = reading(&[b"\x00\x00\x00\x06", b"abcdef"]).with_max_message(5);
    assert_eq!(
        messages.recv().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(messages.get_ref().reads, 1, "nothing read past the header");
    assert_eq!(
        messages.recv().unwrap_err().kind(),
        io::ErrorKind::InvalidData,
        "the next frame cannot be found, so it stays refused"
    );
    assert_eq!(messages.get_ref().reads, 1);
}

#[test]
fn the_longest_declared_length_costs_nothing_until_it_is_sent() {
    let mut messages = reading(&[b"\xff\xff\xff\xff", b"a"]).with_max_message(usize::MAX);
    // The stream ends after one byte: an allocation of 4 GiB here would be seen as a failure to run.
    assert_eq!(
        messages.recv().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn a_message_at_the_bound_passes_and_one_above_it_is_refused_with_nothing_written() {
    let mut messages = Messages::new(Script::default()).with_max_message(3);
    messages.send(b"abc").unwrap();
    let written = messages.get_ref().written.len();
    assert_eq!(
        messages.send(b"abcd").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(messages.get_ref().written.len(), written);
    messages.send(b"abc").unwrap();
}

#[test]
fn a_send_that_stops_inside_a_frame_refuses_every_later_send() {
    let mut messages = Messages::new(Script {
        write_room: Some(2),
        ..Script::default()
    });
    assert_eq!(
        messages.send(b"abc").unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        messages.send(b"x").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn a_send_that_wrote_nothing_can_be_repeated() {
    let mut messages = Messages::new(Script {
        write_room: Some(0),
        ..Script::default()
    });
    assert_eq!(
        messages.send(b"abc").unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(messages.get_ref().written.is_empty());
    assert_eq!(
        messages.send(b"x").unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn a_stream_ending_inside_a_frame_or_between_frames_is_an_unexpected_end() {
    assert_eq!(
        reading(&[b"\x00\x00"]).recv().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        reading(&[b"\x00\x00\x00\x02a"]).recv().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        reading(&[]).recv().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn only_a_bound_above_the_default_is_listed_as_weaker() {
    let script = Script::default;
    assert!(Messages::new(script()).weakened().is_empty());
    assert!(
        Messages::new(script())
            .with_max_message(DEFAULT_MAX_MESSAGE)
            .weakened()
            .is_empty()
    );
    assert!(
        Messages::new(script())
            .with_max_message(1)
            .weakened()
            .is_empty()
    );
    let weakened = Messages::new(script())
        .with_max_message(DEFAULT_MAX_MESSAGE + 1)
        .weakened();
    assert_eq!(weakened[0].setting, "max_message");
}

proptest! {
    #[test]
    fn arbitrary_bytes_give_messages_or_an_error_and_never_a_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096),
        max in 0usize..4096,
    ) {
        let mut messages = reading(&[&bytes]).with_max_message(max);
        while messages.recv().is_ok() {}
    }

    #[test]
    fn any_chunking_of_any_frames_gives_the_same_messages(
        sent in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 0..8),
        chunk in 1usize..64,
    ) {
        let mut writer = Messages::new(Script::default());
        for message in &sent {
            writer.send(message).unwrap();
        }
        let wire = &writer.get_ref().written;
        let chunks: Vec<&[u8]> = wire.chunks(chunk).collect();
        let mut reader = reading(&chunks);
        for message in &sent {
            prop_assert_eq!(&reader.recv().unwrap(), message);
        }
        prop_assert_eq!(reader.recv().unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }
}
