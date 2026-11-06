//! The tokio forms: the same checks, real handshakes of both methods, deadlines and cancellation.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use coqui_peer::asynch::{
    MessageTransport, drive_messages, drive_messages_until, drive_stream, drive_stream_until,
};
use coqui_peer::{DriveError, Method};
use coqui_peer_noise::{Identity as NoiseIdentity, Noise, Trust as NoiseTrust};
use coqui_peer_tls::{Identity as TlsIdentity, Pins, Tls, Trust as TlsTrust};
use coqui_transport_pipe::asynch::{Listener, Messages, Stream};
use coqui_transport_pipe::{Error, current_user};
use support::place;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn assert_send<T: Send>() {}

async fn pair(tag: &str) -> (support::Place, Listener, Stream, Stream) {
    let place = place(tag);
    let mut listener = Listener::bind(&place.name).unwrap();
    let client = Stream::connect(&place.name).await.unwrap();
    let server = listener.accept().await.unwrap();
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

#[test]
fn handles_are_send() {
    assert_send::<Listener>();
    assert_send::<Stream>();
    assert_send::<Messages<Stream>>();
}

#[tokio::test]
async fn a_stream_taken_from_an_await_is_held_across_another() {
    let (_place, _listener, client, _server) = pair("send").await;
    let task = tokio::spawn(async move {
        let mut client = client;
        tokio::task::yield_now().await;
        client.write_all(b"x").await.unwrap();
    });
    task.await.unwrap();
}

#[tokio::test]
async fn bytes_cross_both_ways_and_what_was_written_before_a_close_is_read() {
    let (_place, _listener, mut client, mut server) = pair("bytes").await;
    client.write_all(b"ping").await.unwrap();
    let mut buffer = [0; 4];
    server.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&buffer, b"ping");
    server.write_all(b"last words").await.unwrap();
    drop(server);
    let mut received = Vec::new();
    client.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, b"last words");
}

#[tokio::test]
async fn many_clients_are_served_in_turn() {
    let place = place("many");
    let mut listener = Listener::bind(&place.name).unwrap();
    let name = place.name.clone();
    let clients = tokio::spawn(async move {
        for index in 0..20u8 {
            let mut client = Stream::connect(&name).await.unwrap();
            client.write_all(&[index]).await.unwrap();
            assert_eq!(client.read_u8().await.unwrap(), index);
        }
    });
    for _ in 0..20 {
        let mut server = listener.accept().await.unwrap();
        let byte = server.read_u8().await.unwrap();
        server.write_all(&[byte]).await.unwrap();
    }
    clients.await.unwrap();
}

#[tokio::test]
async fn the_blocking_and_async_forms_talk_to_each_other() {
    let place = place("mixed");
    let mut listener = Listener::bind(&place.name).unwrap();
    let name = place.name.clone();
    let client = tokio::task::spawn_blocking(move || {
        use std::io::{Read, Write};
        let mut client = coqui_transport_pipe::Stream::connect(&name).unwrap();
        client.write_all(b"x").unwrap();
        let mut byte = [0];
        client.read_exact(&mut byte).unwrap();
        byte[0]
    });
    let mut server = listener.accept().await.unwrap();
    assert_eq!(server.read_u8().await.unwrap(), b'x');
    server.write_all(b"y").await.unwrap();
    assert_eq!(client.await.unwrap(), b'y');
}

#[tokio::test]
async fn a_second_listener_is_refused() {
    let place = place("second");
    let _listener = Listener::bind(&place.name).unwrap();
    assert!(matches!(Listener::bind(&place.name), Err(Error::NameTaken)));
    assert!(matches!(
        coqui_transport_pipe::Listener::bind(&place.name),
        Err(Error::NameTaken)
    ));
}

#[tokio::test]
async fn connecting_where_nothing_listens_fails_at_once() {
    let place = place("nobody");
    let result = Stream::connect(&place.name).await;
    assert!(matches!(result, Err(Error::NotListening)), "{result:?}");
}

#[tokio::test]
async fn cancelling_an_accept_loses_nothing() {
    let place = place("cancel");
    let mut listener = Listener::bind(&place.name).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    let mut client = Stream::connect(&place.name).await.unwrap();
    client.write_all(b"x").await.unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!(server.read_u8().await.unwrap(), b'x');
}

#[tokio::test]
async fn each_end_reports_the_other() {
    let (_place, _listener, client, server) = pair("identity").await;
    let me = current_user().unwrap();
    let seen = server.peer().unwrap();
    assert_eq!(seen.user.as_deref(), Some(me.as_str()));
    assert_eq!(seen.process, support::expected_process());
    assert_eq!(client.peer().unwrap().user.as_deref(), Some(me.as_str()));
}

#[tokio::test]
async fn messages_cross_and_a_cancelled_receive_keeps_what_arrived() {
    let (_place, _listener, mut client, server) = pair("messages").await;
    let mut messages = Messages::new(server);
    client.write_all(b"\x00\x00\x00\x04ab").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), messages.recv())
            .await
            .is_err()
    );
    client.write_all(b"cd").await.unwrap();
    assert_eq!(messages.recv().await.unwrap(), b"abcd");
}

#[tokio::test]
async fn a_frame_above_the_bound_is_refused_by_both_ends() {
    let (_place, _listener, client, server) = pair("oversize").await;
    let mut small = Messages::new(client).with_max_message(4);
    assert_eq!(
        small.send(b"too long").await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    let mut large = Messages::new(server);
    large.send(b"too long").await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            small.recv().await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
    assert!(large.weakened().is_empty());
}

#[tokio::test]
async fn a_send_cancelled_after_it_began_refuses_every_later_send() {
    let (_place, _listener, client, _server) = pair("cancel-send").await;
    let mut messages = Messages::new(client).with_max_message(16 * 1024 * 1024);
    let big = vec![0; 16 * 1024 * 1024];
    // tokio's named pipe holds one write in a buffer of its own, so the first send may finish.
    let mut cancelled = false;
    for _ in 0..4 {
        if tokio::time::timeout(Duration::from_millis(100), messages.send(&big))
            .await
            .is_err()
        {
            cancelled = true;
            break;
        }
    }
    assert!(
        cancelled,
        "the peer reads nothing, so these cannot all finish"
    );
    assert_eq!(
        messages.send(b"x").await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noise_proves_both_keys_over_a_stream() {
    let (initiator, responder, id_a, id_b) = noises();
    let (_place, _listener, mut client, mut server) = pair("noise-stream").await;
    let answering = tokio::spawn(async move {
        drive_stream(responder.respond(None).unwrap(), &mut server)
            .await
            .unwrap()
            .peer
    });
    let dialled = drive_stream(initiator.initiate().unwrap(), &mut client)
        .await
        .unwrap()
        .peer;
    assert_eq!(dialled, id_b);
    assert_eq!(answering.await.unwrap(), id_a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noise_proves_both_keys_over_messages_under_a_deadline() {
    let (initiator, responder, id_a, id_b) = noises();
    let (_place, _listener, client, server) = pair("noise-messages").await;
    let until = Instant::now() + Duration::from_secs(30);
    let answering = tokio::spawn(async move {
        drive_messages_until(
            responder.respond(None).unwrap(),
            &mut Messages::new(server),
            until,
        )
        .await
        .unwrap()
        .peer
    });
    let dialled = drive_messages(initiator.initiate().unwrap(), &mut Messages::new(client))
        .await
        .unwrap()
        .peer;
    assert_eq!(dialled, id_b);
    assert_eq!(answering.await.unwrap(), id_a);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_proves_both_certificates_over_a_stream() {
    let (a, b) = (
        TlsIdentity::generate().unwrap(),
        TlsIdentity::generate().unwrap(),
    );
    let pin = |identity: &TlsIdentity| TlsTrust::Pinned(Pins::fixed([identity.fingerprint()]));
    let client_method = Tls::new().with_client(Arc::new(
        coqui_peer_tls::client_config(&a, &pin(&b), &[]).unwrap(),
    ));
    let server_method = Tls::new().with_server(Arc::new(
        coqui_peer_tls::server_config(&b, &pin(&a), &[]).unwrap(),
    ));
    let (_place, _listener, mut client, mut server) = pair("tls-stream").await;
    let answering = tokio::spawn(async move {
        drive_stream(server_method.respond(None).unwrap(), &mut server)
            .await
            .unwrap()
            .peer
    });
    let dialled = drive_stream(client_method.initiate().unwrap(), &mut client)
        .await
        .unwrap()
        .peer;
    assert_eq!(dialled, b.fingerprint());
    assert_eq!(answering.await.unwrap(), a.fingerprint());
}

#[tokio::test]
async fn a_deadline_is_met_on_a_silent_peer() {
    let (initiator, _responder, _, _) = noises();
    let (_place, _listener, mut client, _server) = pair("silent").await;
    let started = Instant::now();
    let until = started + Duration::from_millis(200);
    let result = drive_stream_until(initiator.initiate().unwrap(), &mut client, until).await;
    assert!(matches!(result, Err(DriveError::TimedOut)));
    assert!(started.elapsed() >= Duration::from_millis(190));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[cfg(windows)]
mod windows {
    use coqui_transport_pipe::{ListenerOptions, PipeName, Weakening};

    use super::*;

    #[tokio::test]
    async fn a_client_through_file_sharing_is_refused_by_default() {
        let place = place("remote");
        let _listener = Listener::bind(&place.name).unwrap();
        let text = place.name.path().to_str().unwrap();
        let local = text.strip_prefix(r"\\.\pipe\").unwrap();
        let remote = PipeName::on_host("localhost", local).unwrap();
        let result = Stream::connect(&remote).await;
        assert!(matches!(result, Err(Error::AccessDenied)), "{result:?}");
    }

    #[tokio::test]
    async fn a_descriptor_that_shuts_out_its_own_user_is_refused_at_bind() {
        let place = place("shut-out");
        let result = Listener::bind_with(
            &place.name,
            ListenerOptions::new().security_descriptor("D:P"),
        );
        assert!(
            matches!(result, Err(Error::InvalidDescriptor)),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn weakened_settings_are_listed() {
        let place = place("weakened");
        let listener = Listener::bind_with(
            &place.name,
            ListenerOptions::new().allow_remote_clients(true),
        )
        .unwrap();
        assert_eq!(
            listener.weakened(),
            [Weakening::new("remote_clients", "allowed", "refused")]
        );
    }
}

#[cfg(unix)]
mod unix {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    use coqui_transport_pipe::PipeName;

    use super::*;

    #[tokio::test]
    async fn a_socket_is_its_owners_alone_and_goes_with_its_listener() {
        let place = place("mode");
        let listener = Listener::bind(&place.name).unwrap();
        let metadata = fs::symlink_metadata(place.name.path()).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o600);
        drop(listener);
        assert!(!place.name.path().exists());
    }

    #[tokio::test]
    async fn a_directory_others_can_write_is_refused() {
        let directory = support::private_directory();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o777)).unwrap();
        let name = PipeName::at(directory.path().join("s.sock")).unwrap();
        let result = Listener::bind(&name);
        assert!(matches!(result, Err(Error::SharedDirectory)), "{result:?}");
    }
}

#[tokio::test]
async fn an_async_stream_channel_runs_on_the_stream_through_the_connector() {
    use coqui_transport::asynch::{AsyncChannel, AsyncConnector, StreamChannel};
    use coqui_transport::frame::PeerFrame;
    use coqui_transport_pipe::asynch::Connector;

    let place = place("async-channel");
    let mut listener = Listener::bind(&place.name).unwrap();
    let connector = Connector::new(place.name.clone());
    let client = connector.connect(Duration::from_secs(5)).await.unwrap();
    let server = listener.accept().await.unwrap();
    let client = StreamChannel::new(client, PeerFrame);
    let server = StreamChannel::new(server, PeerFrame);
    client.send(b"over a pipe").await.unwrap();
    let wait = Some(Duration::from_secs(10));
    assert_eq!(server.receive(wait).await.unwrap().unwrap(), b"over a pipe");
    server.send(b"").await.unwrap();
    assert_eq!(client.receive(wait).await.unwrap().unwrap(), b"");
    // On Windows `poll_shutdown` ends nothing, and dropping the stream is the end.
    client.close().await;
    drop(client);
    assert_eq!(server.receive(wait).await, Ok(None));
}

#[tokio::test]
async fn the_async_connector_takes_its_timeout_from_the_call() {
    use coqui_transport::asynch::AsyncConnector;
    use coqui_transport_pipe::asynch::Connector;

    let place = place("async-nobody");
    let started = std::time::Instant::now();
    let error = Connector::new(place.name.clone())
        .connect(Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::NotListening | Error::TimedOut),
        "{error:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(4));
}
