//! `coqui_transport::StreamChannel` over a pipe or a socket, and the wire it puts there.

mod support;

use std::io::{Read, Write};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use coqui_transport::frame::{Codec, PeerFrame};
use coqui_transport::{Channel, Failure, StreamChannel};
use coqui_transport_pipe::{Connector, Listener, Stream};
use serde_json::Value;
use support::place;

const WAIT: Option<Duration> = Some(Duration::from_secs(10));

fn assert_send_sync<T: Send + Sync>() {}

fn streams(tag: &str) -> (support::Place, Listener, Stream, Stream) {
    let place = place(tag);
    let listener = Listener::bind(&place.name).unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let server = listener.accept().unwrap();
    (place, listener, client, server)
}

fn channels(tag: &str) -> (support::Place, Listener, StreamChannel, StreamChannel) {
    let (place, listener, client, server) = streams(tag);
    (
        place,
        listener,
        StreamChannel::start(client, PeerFrame).unwrap(),
        StreamChannel::start(server, PeerFrame).unwrap(),
    )
}

#[test]
fn a_stream_channel_on_a_pipe_is_send_and_sync() {
    assert_send_sync::<StreamChannel>();
}

#[test]
fn messages_cross_in_order_both_ways() {
    let (_place, _listener, client, server) = channels("order");
    for index in 0..100u8 {
        client.send(&[index]).unwrap();
    }
    for index in 0..100u8 {
        assert_eq!(server.receive(WAIT).unwrap().unwrap(), [index]);
    }
    server.send(b"answer").unwrap();
    assert_eq!(client.receive(WAIT).unwrap().unwrap(), b"answer");
}

#[test]
fn an_empty_message_and_a_large_one_cross() {
    let (_place, _listener, client, server) = channels("sizes");
    let large: Vec<u8> = (0..1_000_000u32).map(|n| (n % 251) as u8).collect();
    client.send(&[]).unwrap();
    client.send(&large).unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), Vec::<u8>::new());
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), large);
}

#[test]
fn the_peer_is_asked_before_the_stream_is_given_to_a_channel() {
    let (_place, _listener, client, server) = streams("peer");
    let identity = server.peer().unwrap();
    assert_eq!(identity.process, support::expected_process());
    let _channel = StreamChannel::start(client, PeerFrame).unwrap();
}

#[test]
fn closing_sends_what_is_queued_and_the_peer_reads_the_end_in_order() {
    let (_place, _listener, client, server) = channels("close");
    for index in 0..50u8 {
        client.send(&[index; 1000]).unwrap();
    }
    client.close();
    for index in 0..50u8 {
        assert_eq!(server.receive(WAIT).unwrap().unwrap(), vec![index; 1000]);
    }
    assert_eq!(server.receive(WAIT), Ok(None));
    assert_eq!(server.receive(WAIT), Ok(None));
}

#[test]
fn closing_with_what_the_peer_sent_unread_does_not_lose_what_was_sent_last() {
    let (_place, _listener, client, server) = streams("linger");
    let client = StreamChannel::builder(PeerFrame)
        .pending(1)
        .start(client)
        .unwrap();
    let server = StreamChannel::start(server, PeerFrame).unwrap();
    for _ in 0..20 {
        server.send(&[1; 10_000]).unwrap();
    }
    thread::sleep(Duration::from_millis(200));
    client.send(b"final").unwrap();
    client.close();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"final");
    assert_eq!(server.receive(WAIT), Ok(None));
}

#[test]
fn closing_wakes_a_blocked_receive_and_can_be_repeated() {
    let (_place, _listener, client, _server) = channels("wake");
    let client = Arc::new(client);
    let waiting = {
        let client = client.clone();
        thread::spawn(move || client.receive(None))
    };
    thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    client.close();
    // The reader is woken by the stream's shutdown, so no thread is waited for up to the send limit.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    client.close();
    assert_eq!(waiting.join().unwrap(), Ok(None));
    assert_eq!(client.send(b"x"), Err(Failure::PathLost));
}

#[test]
fn a_peer_that_left_inside_a_frame_is_a_lost_path() {
    let (_place, _listener, client, server) = streams("torn");
    let server = StreamChannel::start(server, PeerFrame).unwrap();
    (&client).write_all(&[0, 0, 0, 9, 1, 2]).unwrap();
    drop(client);
    assert_eq!(server.receive(WAIT), Err(Failure::PathLost));
}

#[test]
fn a_message_above_the_receive_bound_ends_the_channel_at_both_ends() {
    let (_place, _listener, client, server) = streams("bound");
    let client = StreamChannel::start(client, PeerFrame).unwrap();
    let server = StreamChannel::builder(PeerFrame)
        .max_message(8)
        .start(server)
        .unwrap();
    client.send(&[7; 64]).unwrap();
    assert_eq!(server.receive(WAIT), Err(Failure::Protocol));
    assert!(matches!(
        client.receive(WAIT),
        Ok(None) | Err(Failure::PathLost)
    ));
}

#[test]
fn a_peer_that_takes_nothing_for_the_send_limit_ends_the_channel() {
    let place = place("stall");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let client = StreamChannel::builder(PeerFrame)
        .send_limit(Duration::from_millis(200))
        .start(Stream::connect(&place.name).unwrap())
        .unwrap();
    let _server = listener.accept().unwrap();
    let started = Instant::now();
    let failure = loop {
        if let Err(failure) = client.send(&[0; 256 * 1024]) {
            break failure;
        }
        assert!(started.elapsed() < Duration::from_secs(20), "never ended");
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(failure, Failure::NoProgress);
}

#[test]
fn a_receive_times_out_keeping_the_channel() {
    let (_place, _listener, client, server) = channels("timeout");
    assert_eq!(
        client.receive(Some(Duration::from_millis(50))),
        Err(Failure::Timeout)
    );
    server.send(b"late").unwrap();
    assert_eq!(client.receive(WAIT).unwrap().unwrap(), b"late");
}

#[test]
fn the_connector_and_the_listener_traits_give_streams_a_channel_runs_on() {
    use coqui_transport::Connector as _;

    let place = place("traits");
    let listener = Listener::bind(&place.name).unwrap();
    let connector = Connector::new(place.name.clone());
    let client = connector.connect(Duration::from_secs(5)).unwrap();
    let server = coqui_transport::Listener::accept(&listener).unwrap();
    let client = StreamChannel::start(client, PeerFrame).unwrap();
    let server = StreamChannel::start(server, PeerFrame).unwrap();
    client.send(b"through the traits").unwrap();
    assert_eq!(
        server.receive(WAIT).unwrap().unwrap(),
        b"through the traits"
    );
}

#[test]
fn the_connector_gives_its_timeout_to_the_connect() {
    use coqui_transport::Connector as _;

    // Nothing accepts, so the queue fills (on Windows, the instances) and a connect waits for room.
    let place = place("full");
    let _listener = Listener::options().backlog(1).bind(&place.name).unwrap();
    let connector = Connector::new(place.name.clone());
    let mut held = Vec::new();
    for _ in 0..300 {
        let started = Instant::now();
        match connector.connect(Duration::from_millis(300)) {
            Ok(stream) => held.push(stream),
            Err(error) => {
                assert!(
                    matches!(error, coqui_transport_pipe::Error::TimedOut),
                    "{error:?}"
                );
                let waited = started.elapsed();
                assert!(waited >= Duration::from_millis(250), "{waited:?}");
                assert!(waited < Duration::from_secs(4), "{waited:?}");
                return;
            }
        }
    }
    panic!("the queue never filled");
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

fn vectors() -> Value {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/peer_frame.json");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn max(case: &Value) -> usize {
    usize::try_from(case["max"].as_u64().unwrap()).unwrap()
}

fn case_name(case: &Value) -> &str {
    case["name"].as_str().unwrap()
}

fn flag(case: &Value, key: &str) -> bool {
    case.get(key).and_then(Value::as_bool) == Some(true)
}

/// What a channel sends over a real pipe reaches the other end as the vector's bytes, and what it
/// refuses reaches it not at all.
#[test]
fn what_a_channel_puts_on_a_pipe_is_the_vector_bytes() {
    let vectors = vectors();
    for case in vectors["encode"].as_array().unwrap() {
        let (_place, _listener, mine, mut raw) = streams("wire-out");
        let channel = StreamChannel::builder(PeerFrame)
            .max_message(max(case))
            .start(mine)
            .unwrap();
        let body = match (case.get("text"), case.get("body")) {
            (Some(text), _) => text.as_str().unwrap().as_bytes().to_vec(),
            (None, Some(body)) => bytes(body.as_str().unwrap()),
            (None, None) => panic!("{}: no body", case_name(case)),
        };
        let sent = channel.send(&body);
        channel.close();
        let mut wire = Vec::new();
        raw.read_to_end(&mut wire).unwrap();
        if flag(case, "error") {
            assert_eq!(sent, Err(Failure::Protocol), "{}", case_name(case));
            assert!(wire.is_empty(), "{}", case_name(case));
        } else {
            assert_eq!(sent, Ok(()), "{}", case_name(case));
            assert_eq!(
                hex(&wire),
                case["hex"].as_str().unwrap(),
                "{}",
                case_name(case)
            );
        }
    }
}

/// What a channel receives from the vector's bytes, written to a real pipe in the vector's chunks,
/// is the vector's messages, then the end it says: a failure, a torn frame, or an orderly end.
#[test]
fn what_a_channel_takes_off_a_pipe_is_the_vector_messages() {
    let vectors = vectors();
    for case in vectors["decode"].as_array().unwrap() {
        let (_place, _listener, mine, raw) = streams("wire-in");
        let channel = StreamChannel::builder(PeerFrame)
            .max_message(max(case))
            .start(mine)
            .unwrap();
        for chunk in case["chunks"].as_array().unwrap() {
            (&raw).write_all(&bytes(chunk.as_str().unwrap())).unwrap();
        }
        drop(raw);
        let mut got = Vec::new();
        let end = loop {
            match channel.receive(WAIT) {
                Ok(Some(message)) => got.push(message),
                other => break other,
            }
        };
        let expected: Vec<Vec<u8>> = case["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| bytes(message.as_str().unwrap()))
            .collect();
        assert_eq!(got, expected, "{}", case_name(case));
        let expected_end = if flag(case, "error") {
            Err(Failure::Protocol)
        } else if flag(case, "truncated") {
            Err(Failure::PathLost)
        } else {
            Ok(None)
        };
        assert_eq!(end, expected_end, "{}", case_name(case));
    }
}

/// The frame a pipe's `Messages` writes is the codec's, so both ends of one wire agree.
#[test]
fn messages_and_a_channel_share_one_wire() {
    use coqui_transport_pipe::{MessageTransport, Messages};

    let (_place, _listener, client, server) = streams("shared-wire");
    let mut messages = Messages::new(client);
    let channel = StreamChannel::start(server, PeerFrame).unwrap();
    messages.send(b"from messages").unwrap();
    assert_eq!(channel.receive(WAIT).unwrap().unwrap(), b"from messages");
    channel.send(b"from the channel").unwrap();
    assert_eq!(messages.recv().unwrap(), b"from the channel");
    assert_eq!(
        PeerFrame.encode(b"x", 8).unwrap(),
        [0, 0, 0, 1, b'x'],
        "the codec both use"
    );
}
