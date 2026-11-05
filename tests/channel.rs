//! The channel over a stream, and a `coqui-agentic-peer` link running on it.

mod support;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use coqui_agentic_peer::auth::{Identity, Method, Proof, Sealed, Trust, prove, prove_out};
use coqui_agentic_peer::link::{self, Host, Options, Services};
use coqui_agentic_peer::mux::{Limits, Mux, Side};
use coqui_agentic_peer::peers::{Events, MemoryPeers, NoEvents, PeerRecord, PeerStore};
use coqui_peer_pipe::channel::{Channel, ChannelOptions, DEFAULT_QUEUED_BYTES};
use coqui_peer_pipe::{Listener, Stream};
use coqui_web::{Channel as _, Failure};
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

fn channels(tag: &str) -> (support::Place, Listener, Channel, Channel) {
    let (place, listener, client, server) = streams(tag);
    (
        place,
        listener,
        Channel::start(client).unwrap(),
        Channel::start(server).unwrap(),
    )
}

#[test]
fn a_channel_is_send_and_sync() {
    assert_send_sync::<Channel>();
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
    assert_eq!(server.peer().unwrap().process, Some(std::process::id()));
}

#[test]
fn an_empty_message_and_a_large_one_cross() {
    let (_place, _listener, client, server) = channels("sizes");
    client.send(b"").unwrap();
    client.send(&vec![7; 1024 * 1024]).unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"");
    assert_eq!(server.receive(WAIT).unwrap().unwrap().len(), 1024 * 1024);
}

#[test]
fn a_receive_times_out_keeping_what_arrived_and_the_channel_goes_on() {
    let (_place, _listener, client, server) = streams("timeout");
    let mut client = client;
    let server = Channel::start(server).unwrap();
    use std::io::Write;
    client.write_all(b"\x00\x00\x00\x04ab").unwrap();
    let started = Instant::now();
    assert_eq!(
        server.receive(Some(Duration::from_millis(100))),
        Err(Failure::Timeout)
    );
    assert!(started.elapsed() >= Duration::from_millis(90));
    client.write_all(b"cd").unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"abcd");
}

#[test]
fn a_peer_closing_in_order_is_none_and_stays_none() {
    let (_place, _listener, client, server) = channels("orderly");
    client.send(b"bye").unwrap();
    client.close();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"bye");
    assert_eq!(server.receive(WAIT), Ok(None));
    assert_eq!(server.receive(WAIT), Ok(None));
}

#[test]
fn a_connection_ending_inside_a_frame_is_a_lost_path() {
    let (_place, _listener, client, server) = streams("lost");
    let mut client = client;
    let server = Channel::start(server).unwrap();
    use std::io::Write;
    client.write_all(b"\x00\x00\x00\x04ab").unwrap();
    drop(client);
    assert_eq!(server.receive(WAIT), Err(Failure::PathLost));
    assert_eq!(server.receive(WAIT), Err(Failure::PathLost));
}

#[test]
fn a_message_above_the_send_bound_is_refused_and_the_channel_goes_on() {
    let (_place, _listener, client, server) = channels("send-bound");
    client.set_max_send(4);
    assert_eq!(client.send(b"too long"), Err(Failure::Protocol));
    client.send(b"fits").unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"fits");
}

#[test]
fn a_message_above_the_receive_bound_ends_the_channel_at_both_ends() {
    let (_place, _listener, client, server) = channels("receive-bound");
    server.set_max_receive(4);
    client.send(b"too long").unwrap();
    assert_eq!(server.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(server.receive(WAIT), Err(Failure::Protocol));
    assert_eq!(client.receive(WAIT), Ok(None));
}

#[test]
fn a_staged_bound_takes_effect_by_position() {
    let (_place, _listener, client, server) = channels("staged");
    server.set_max_receive(4);
    server.set_max_receive_after(2, 16).unwrap();
    client.send(b"abcd").unwrap();
    client.send(b"efgh").unwrap();
    client.send(b"sixteen-is-more!").unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"abcd");
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"efgh");
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"sixteen-is-more!");
    client.send(&[0; 17]).unwrap();
    assert_eq!(server.receive(WAIT), Err(Failure::Protocol));
}

#[test]
fn a_stage_of_nothing_is_the_bound_at_once_and_a_later_call_replaces_a_stage() {
    let (_place, _listener, client, server) = channels("staged-replaced");
    server.set_max_receive_after(1, 1).unwrap();
    server.set_max_receive_after(0, 4).unwrap();
    client.send(b"abcd").unwrap();
    client.send(b"efgh").unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"abcd");
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"efgh");
}

#[test]
fn a_full_queue_refuses_or_waits_and_nothing_refused_is_sent() {
    let place = place("full");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let server = Channel::start(listener.accept().unwrap()).unwrap();
    let client = ChannelOptions::new()
        .queued_bytes(256 * 1024)
        .max_message(128 * 1024)
        .start(client)
        .unwrap();
    let message = vec![1; 100 * 1024];
    // The socket or pipe buffers fill first; then the queue.
    let started = Instant::now();
    let mut queued = 0;
    let refused = loop {
        match client.send(&message) {
            Ok(()) => queued += 1,
            Err(failure) => break failure,
        }
        assert!(started.elapsed() < Duration::from_secs(20), "never full");
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(refused, Failure::NoProgress);
    assert_eq!(
        client.send_timeout(&message, Some(Duration::from_millis(100))),
        Err(Failure::Timeout)
    );
    assert_eq!(
        client.send_timeout(&vec![0; 300 * 1024], None).unwrap_err(),
        Failure::Protocol,
        "above the message bound"
    );
    // Every message queued arrives whole, and the refused ones do not.
    server.set_max_receive(128 * 1024);
    for _ in 0..queued {
        assert_eq!(server.receive(WAIT).unwrap().unwrap(), message);
    }
    client.send_timeout(b"after", WAIT).unwrap();
    assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"after");
}

#[test]
fn a_message_larger_than_the_whole_queue_is_refused_at_once() {
    let (_place, _listener, client, _server) = streams("budget");
    let client = ChannelOptions::new()
        .queued_bytes(16)
        .max_message(1024)
        .start(client)
        .unwrap();
    assert_eq!(
        client.send_timeout(&[0; 64], None),
        Err(Failure::NoProgress)
    );
}

#[test]
fn a_peer_that_takes_nothing_for_the_send_limit_ends_the_channel() {
    let place = place("stall");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let client = ChannelOptions::new()
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
fn closing_sends_what_is_queued() {
    let (_place, _listener, client, server) = channels("drain");
    let reader = thread::spawn(move || {
        let mut count = 0;
        while let Some(message) = server.receive(WAIT).unwrap() {
            assert_eq!(message.len(), 64 * 1024);
            count += 1;
        }
        count
    });
    for _ in 0..64 {
        client.send(&[1; 64 * 1024]).unwrap();
    }
    client.close();
    assert_eq!(reader.join().unwrap(), 64);
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
    client.close();
    client.close();
    assert_eq!(waiting.join().unwrap(), Ok(None));
    assert_eq!(client.send(b"x"), Err(Failure::PathLost));
}

#[test]
fn a_second_receiver_waits_its_turn_up_to_its_timeout() {
    let (_place, _listener, client, _server) = channels("turn");
    let client = Arc::new(client);
    let first = {
        let client = client.clone();
        thread::spawn(move || client.receive(None))
    };
    thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    assert_eq!(
        client.receive(Some(Duration::from_millis(100))),
        Err(Failure::Timeout)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    client.close();
    first.join().unwrap().unwrap();
}

#[test]
fn only_settings_above_their_defaults_are_listed_as_weaker() {
    assert!(ChannelOptions::new().weakened().is_empty());
    let weakened = ChannelOptions::new()
        .queued_bytes(DEFAULT_QUEUED_BYTES + 1)
        .send_limit(Duration::from_secs(31))
        .max_message(2 * 1024 * 1024)
        .weakened();
    let names: Vec<_> = weakened
        .iter()
        .map(|weaker| weaker.setting.as_str())
        .collect();
    assert_eq!(names, ["max_message", "queued_bytes", "send_limit"]);
}

/// Both proofs over two channels of a pipe: the server's and the client's, each knowing the other's id.
fn proved(
    client: &Channel,
    server: &Channel,
    method: Method,
) -> (
    coqui_peer::Established<coqui_agentic_peer::auth::Session>,
    coqui_peer::Established<coqui_agentic_peer::auth::Session>,
    String,
    String,
) {
    let (a, b) = (
        Identity::generate(method).unwrap(),
        Identity::generate(method).unwrap(),
    );
    let (id_a, id_b) = (a.peer_id(), b.peer_id());
    let dialling = Proof::new(a).with_responders(Trust::pinned([id_b]));
    let answering = Proof::new(b).with_initiators(Trust::pinned([id_a]));
    let limit = Duration::from_secs(20);
    let max = 1024 * 1024;
    thread::scope(|scope| {
        let answered = scope.spawn(|| prove(&answering, server, limit, max).unwrap());
        let dialled = prove_out(&dialling, client, limit, max).unwrap();
        (
            dialled,
            answered.join().unwrap(),
            id_a.to_string(),
            id_b.to_string(),
        )
    })
}

#[test]
fn a_proof_over_the_channel_gives_a_sealed_channel_of_both_methods() {
    for method in Method::ALL {
        let (_place, _listener, client, server) = channels("sealed");
        let (dialled, answered, id_a, id_b) = proved(&client, &server, method);
        assert_eq!(dialled.peer.to_string(), id_b);
        assert_eq!(answered.peer.to_string(), id_a);
        let client = Sealed::new(&client, dialled.session);
        let server = Sealed::new(&server, answered.session);
        client.send(b"sealed").unwrap();
        assert_eq!(server.receive(WAIT).unwrap().unwrap(), b"sealed");
        server.send(b"both ways").unwrap();
        assert_eq!(client.receive(WAIT).unwrap().unwrap(), b"both ways");
    }
}

#[test]
fn a_stranger_is_refused_by_the_proof_over_the_channel() {
    let (_place, _listener, client, server) = channels("stranger");
    let (stranger, known, responder) = (
        Identity::generate(Method::Noise).unwrap(),
        Identity::generate(Method::Noise).unwrap(),
        Identity::generate(Method::Noise).unwrap(),
    );
    let dialling = Proof::new(stranger).with_responders(Trust::pinned([responder.peer_id()]));
    let answering = Proof::new(responder).with_initiators(Trust::pinned([known.peer_id()]));
    let limit = Duration::from_secs(20);
    thread::scope(|scope| {
        let answered = scope.spawn(|| prove(&answering, &server, limit, 1024).is_err());
        let _ = prove_out(&dialling, &client, limit, 1024);
        assert!(answered.join().unwrap());
    });
}

#[test]
fn a_link_runs_on_a_sealed_channel_over_a_pipe() {
    let (_place, _listener, client, server) = channels("link");
    let (dialled, answered, id_a, _id_b) = proved(&client, &server, Method::Noise);
    let client = Sealed::new(&client, dialled.session);
    let server = Sealed::new(&server, answered.session);

    let store = MemoryPeers::default();
    store
        .put(&PeerRecord::new(&id_a).granting("echo/1"))
        .unwrap();
    let services = Services::default().with(
        "echo/1",
        Box::new(|stream, _accepted| {
            while let Ok(Some(message)) = stream.receive(WAIT) {
                if stream.send(&message).is_err() {
                    return;
                }
            }
        }),
    );
    let events: &dyn Events = &NoEvents;
    let result = Arc::new(Mutex::new(None));
    thread::scope(|scope| {
        let serving = scope.spawn(|| {
            link::serve(
                &server,
                Side::Answered,
                Options::default(),
                None,
                &id_a,
                &Host {
                    store: &store,
                    services: &services,
                    events,
                },
            );
        });
        let result = result.clone();
        Mux::run(&client, Side::Dialed, Limits::default(), None, move |mux| {
            let capabilities = link::capabilities(mux, Duration::from_secs(10)).unwrap();
            assert!(capabilities.offers.iter().any(|offer| offer == "echo/1"));
            let stream = mux.open("echo/1").unwrap();
            stream.send(b"over a pipe").unwrap();
            *result.lock().unwrap() = Some(stream.receive(WAIT).unwrap().unwrap());
            stream.close();
        });
        // The link is over: closing the channels ends what serves it.
        client.close();
        serving.join().unwrap();
    });
    assert_eq!(result.lock().unwrap().take().unwrap(), b"over a pipe");
}
