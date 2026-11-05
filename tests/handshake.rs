//! Real handshakes of the two methods over a stream and over messages, with and without a deadline.

mod support;

use std::io::Read;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use coqui_peer::{
    DriveError, Method, drive_messages, drive_messages_until, drive_stream, drive_stream_until,
};
use coqui_peer_noise::{Identity as NoiseIdentity, Noise, Trust as NoiseTrust};
use coqui_peer_pipe::{Listener, Messages, Stream};
use coqui_peer_tls::{Identity as TlsIdentity, Pins, Tls, Trust as TlsTrust};
use support::place;

fn connected(tag: &str) -> (support::Place, Listener, Stream, Stream) {
    let place = place(tag);
    let listener = Listener::bind(&place.name).unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let server = listener.accept().unwrap();
    (place, listener, client, server)
}

fn noises() -> (Noise, Noise, coqui_peer::PeerId, coqui_peer::PeerId) {
    let (a, b) = (
        NoiseIdentity::generate().unwrap(),
        NoiseIdentity::generate().unwrap(),
    );
    let (id_a, id_b) = (a.peer_id(), b.peer_id());
    (
        Noise::new(a).with_responders(NoiseTrust::pinned([id_b])),
        Noise::new(b).with_initiators(NoiseTrust::pinned([id_a])),
        id_a,
        id_b,
    )
}

fn tlses() -> (Tls, Tls, coqui_peer::PeerId, coqui_peer::PeerId) {
    let (a, b) = (
        TlsIdentity::generate().unwrap(),
        TlsIdentity::generate().unwrap(),
    );
    let pin = |identity: &TlsIdentity| TlsTrust::Pinned(Pins::fixed([identity.fingerprint()]));
    let client = Tls::new().with_client(Arc::new(
        coqui_peer_tls::client_config(&a, &pin(&b), &[]).unwrap(),
    ));
    let server = Tls::new().with_server(Arc::new(
        coqui_peer_tls::server_config(&b, &pin(&a), &[]).unwrap(),
    ));
    (client, server, a.fingerprint(), b.fingerprint())
}

/// One handshake with each side on its own thread. `drive` is the driver under test.
fn both_sides<M, R>(
    tag: &str,
    initiator: M,
    responder: M,
    drive: impl Fn(<M as Method>::Handshake, Stream) -> R + Send + Sync + 'static,
) -> (R, R)
where
    M: Method + Send + 'static,
    <M as Method>::Handshake: Send + 'static,
    R: Send + 'static,
{
    let (_place, _listener, client, server) = connected(tag);
    let drive = Arc::new(drive);
    let answering = {
        let drive = drive.clone();
        thread::spawn(move || drive(responder.respond(None).unwrap(), server))
    };
    let dialled = drive(initiator.initiate().unwrap(), client);
    (dialled, answering.join().unwrap())
}

#[test]
fn noise_proves_both_keys_over_a_stream() {
    let (initiator, responder, id_a, id_b) = noises();
    let (dialled, answered) =
        both_sides("noise-stream", initiator, responder, |handshake, mut io| {
            drive_stream(handshake, &mut io).unwrap().peer
        });
    assert_eq!(dialled, id_b);
    assert_eq!(answered, id_a);
}

#[test]
fn noise_proves_both_keys_over_messages() {
    let (initiator, responder, id_a, id_b) = noises();
    let (dialled, answered) =
        both_sides("noise-messages", initiator, responder, |handshake, io| {
            drive_messages(handshake, &mut Messages::new(io))
                .unwrap()
                .peer
        });
    assert_eq!(dialled, id_b);
    assert_eq!(answered, id_a);
}

#[test]
fn noise_proves_both_keys_under_a_deadline() {
    let (initiator, responder, id_a, id_b) = noises();
    let (dialled, answered) =
        both_sides("noise-until", initiator, responder, |handshake, mut io| {
            drive_stream_until(handshake, &mut io, Instant::now() + Duration::from_secs(30))
                .unwrap()
                .peer
        });
    assert_eq!(dialled, id_b);
    assert_eq!(answered, id_a);
}

#[test]
fn noise_proves_both_keys_over_messages_under_a_deadline() {
    let (initiator, responder, id_a, id_b) = noises();
    let (dialled, answered) = both_sides(
        "noise-messages-until",
        initiator,
        responder,
        |handshake, io| {
            let until = Instant::now() + Duration::from_secs(30);
            drive_messages_until(handshake, &mut Messages::new(io), until)
                .unwrap()
                .peer
        },
    );
    assert_eq!(dialled, id_b);
    assert_eq!(answered, id_a);
}

#[test]
fn tls_proves_both_certificates_over_a_stream() {
    let (client, server, id_a, id_b) = tlses();
    let (_place, _listener, mut to_server, mut to_client) = connected("tls-stream");
    let answering = thread::spawn(move || {
        drive_stream(server.respond(None).unwrap(), &mut to_client)
            .unwrap()
            .peer
    });
    let dialled = drive_stream(client.initiate().unwrap(), &mut to_server)
        .unwrap()
        .peer;
    assert_eq!(dialled, id_b);
    assert_eq!(answering.join().unwrap(), id_a);
}

#[test]
fn tls_proves_both_certificates_under_a_deadline() {
    let (client, server, id_a, id_b) = tlses();
    let (_place, _listener, mut to_server, mut to_client) = connected("tls-until");
    let until = Instant::now() + Duration::from_secs(30);
    let answering = thread::spawn(move || {
        drive_stream_until(server.respond(None).unwrap(), &mut to_client, until)
            .unwrap()
            .peer
    });
    let dialled = drive_stream_until(client.initiate().unwrap(), &mut to_server, until)
        .unwrap()
        .peer;
    assert_eq!(dialled, id_b);
    assert_eq!(answering.join().unwrap(), id_a);
}

#[test]
fn a_key_nobody_pinned_is_refused_over_a_pipe() {
    let (a, b, stranger) = (
        NoiseIdentity::generate().unwrap(),
        NoiseIdentity::generate().unwrap(),
        NoiseIdentity::generate().unwrap(),
    );
    let initiator = Noise::new(stranger).with_responders(NoiseTrust::pinned([b.peer_id()]));
    let responder = Noise::new(b).with_initiators(NoiseTrust::pinned([a.peer_id()]));
    let (_place, _listener, mut client, mut server) = connected("stranger");
    let answering =
        thread::spawn(move || drive_stream(responder.respond(None).unwrap(), &mut server).is_err());
    let _ = drive_stream(initiator.initiate().unwrap(), &mut client);
    assert!(answering.join().unwrap());
}

#[test]
fn a_deadline_is_met_on_a_silent_peer() {
    let (initiator, _responder, _, _) = noises();
    let (_place, _listener, mut client, _server) = connected("silent");
    let started = Instant::now();
    let until = started + Duration::from_millis(200);
    let result = drive_stream_until(initiator.initiate().unwrap(), &mut client, until);
    assert!(matches!(result, Err(DriveError::TimedOut)));
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(190), "{waited:?}");
    assert!(waited < Duration::from_secs(5), "{waited:?}");
}

#[test]
fn a_deadline_is_met_over_messages_on_a_silent_peer() {
    let (initiator, _responder, _, _) = noises();
    let (_place, _listener, client, _server) = connected("silent-messages");
    let started = Instant::now();
    let until = started + Duration::from_millis(200);
    let result = drive_messages_until(
        initiator.initiate().unwrap(),
        &mut Messages::new(client),
        until,
    );
    assert!(matches!(result, Err(DriveError::TimedOut)));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_deadline_met_leaves_the_stream_without_a_timeout_as_coqui_peer_documents() {
    let (initiator, _responder, _, _) = noises();
    let (_place, _listener, client, _server) = connected("cleared");
    let mut shared = &client;
    let until = Instant::now() + Duration::from_millis(50);
    let _ = drive_stream_until(initiator.initiate().unwrap(), &mut shared, until);
    // A read through the same stream now waits, so it is ended by a shutdown from here.
    let reader = thread::scope(|scope| {
        let reading = scope.spawn(|| {
            let mut stream = &client;
            let started = Instant::now();
            let got = stream.read(&mut [0; 8]);
            (started.elapsed(), got.map_err(|error| error.kind()))
        });
        thread::sleep(Duration::from_millis(200));
        client.shutdown().unwrap();
        reading.join().unwrap()
    });
    assert!(reader.0 >= Duration::from_millis(150), "{reader:?}");
    assert_eq!(reader.1, Ok(0));
}

#[test]
fn a_frame_above_coqui_peers_bound_is_too_long_for_its_driver() {
    let (_initiator, responder, _, _) = noises();
    let (_place, _listener, mut client, mut server) = connected("too-long");
    use std::io::Write;
    client.write_all(&u32::MAX.to_be_bytes()).unwrap();
    let result = drive_stream(responder.respond(None).unwrap(), &mut server);
    assert!(matches!(result, Err(DriveError::TooLong)));
}
