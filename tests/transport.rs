//! The stream and the listener on this platform: bytes, ends, timeouts, identities.

mod support;

use std::io::{self, Read, Write};
use std::thread;
use std::time::{Duration, Instant};

use coqui_transport_pipe::{
    ConnectOptions, Error, Listener, ListenerOptions, MessageTransport, Messages, ReadTimeout,
    Stream, WriteTimeout, current_user,
};
use support::place;

fn assert_send_sync<T: Send + Sync>() {}

fn pair(tag: &str) -> (support::Place, Listener, Stream, Stream) {
    let place = place(tag);
    let listener = Listener::bind(&place.name).unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let server = listener.accept().unwrap();
    (place, listener, client, server)
}

#[test]
fn handles_are_send_and_sync() {
    assert_send_sync::<Listener>();
    assert_send_sync::<Stream>();
    assert_send_sync::<ListenerOptions>();
    assert_send_sync::<ConnectOptions>();
}

#[test]
fn bytes_cross_both_ways() {
    let (_place, _listener, mut client, mut server) = pair("both-ways");
    client.write_all(b"ping").unwrap();
    let mut buffer = [0; 4];
    server.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, b"ping");
    server.write_all(b"pong").unwrap();
    client.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, b"pong");
}

#[test]
fn a_message_larger_than_any_buffer_crosses() {
    let (_place, _listener, client, server) = pair("large");
    let message: Vec<u8> = (0..8 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect();
    let expected = message.clone();
    let writer = thread::spawn(move || {
        let mut client = client;
        client.write_all(&message).unwrap();
    });
    let mut received = vec![0; expected.len()];
    let mut server = server;
    server.read_exact(&mut received).unwrap();
    writer.join().unwrap();
    assert!(received == expected);
}

#[test]
fn many_clients_are_served_in_turn() {
    let place = place("many");
    let listener = Listener::bind(&place.name).unwrap();
    let name = place.name.clone();
    let clients = thread::spawn(move || {
        for index in 0..20u8 {
            let mut client = Stream::connect(&name).unwrap();
            client.write_all(&[index]).unwrap();
            let mut answer = [0];
            client.read_exact(&mut answer).unwrap();
            assert_eq!(answer[0], index);
        }
    });
    for _ in 0..20 {
        let mut server = listener.accept().unwrap();
        let mut byte = [0];
        server.read_exact(&mut byte).unwrap();
        server.write_all(&byte).unwrap();
    }
    clients.join().unwrap();
}

#[test]
fn what_was_written_before_a_close_is_read_and_then_the_end() {
    let (_place, _listener, mut client, server) = pair("linger");
    {
        let mut server = server;
        server.write_all(b"last words").unwrap();
    }
    let mut received = Vec::new();
    client.read_to_end(&mut received).unwrap();
    assert_eq!(received, b"last words");
    assert_eq!(client.read(&mut [0; 8]).unwrap(), 0);
}

#[test]
fn the_same_holds_for_what_a_client_wrote() {
    let (_place, _listener, client, mut server) = pair("linger-client");
    {
        let mut client = client;
        client.write_all(b"last words").unwrap();
    }
    let mut received = Vec::new();
    server.read_to_end(&mut received).unwrap();
    assert_eq!(received, b"last words");
}

#[test]
fn writing_to_a_closed_peer_is_a_broken_pipe() {
    let (_place, _listener, mut client, server) = pair("broken");
    drop(server);
    let error = loop {
        // A socket may take one write before it reports the close.
        if let Err(error) = client.write_all(b"x") {
            break error;
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn an_empty_write_and_an_empty_read_do_nothing() {
    let (_place, _listener, mut client, mut server) = pair("empty");
    assert_eq!(client.write(b"").unwrap(), 0);
    assert_eq!(server.read(&mut []).unwrap(), 0);
    client.write_all(b"x").unwrap();
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"x");
}

#[test]
fn a_read_timeout_ends_a_read_on_a_silent_peer_and_loses_nothing() {
    let (_place, _listener, mut client, mut server) = pair("read-timeout");
    server
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let started = Instant::now();
    let error = server.read(&mut [0; 8]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(90), "{waited:?}");
    assert!(waited < Duration::from_secs(5), "{waited:?}");

    client.write_all(b"late").unwrap();
    let mut buffer = [0; 4];
    server.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, b"late");
}

#[test]
fn no_read_timeout_waits_past_where_one_would_end() {
    let (_place, _listener, mut client, mut server) = pair("no-timeout");
    server.set_read_timeout(None).unwrap();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        client.write_all(b"x").unwrap();
    });
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
    writer.join().unwrap();
}

#[test]
fn a_zero_timeout_is_refused_as_it_is_on_a_socket() {
    let (_place, _listener, client, _server) = pair("zero");
    let zero = Some(Duration::ZERO);
    assert_eq!(
        client.set_read_timeout(zero).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        client.set_write_timeout(zero).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn a_write_timeout_ends_a_write_the_peer_never_takes() {
    let place = place("write-timeout");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let mut client = Stream::connect(&place.name).unwrap();
    let _server = listener.accept().unwrap();
    client
        .set_write_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let started = Instant::now();
    let data = vec![0; 1024 * 1024];
    let error = loop {
        // A socket's buffer fills first.
        if let Err(error) = client.write(&data) {
            break error;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the write never stopped"
        );
    };
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
}

#[test]
fn a_shutdown_wakes_a_blocked_read_and_the_peer_sees_the_end() {
    let (_place, _listener, mut client, server) = pair("shutdown");
    let server = std::sync::Arc::new(server);
    server.set_read_timeout(None).unwrap();
    let reader = {
        let server = server.clone();
        thread::spawn(move || {
            let mut server = &*server;
            server.read(&mut [0; 8]).unwrap()
        })
    };
    thread::sleep(Duration::from_millis(100));
    server.shutdown().unwrap();
    assert_eq!(reader.join().unwrap(), 0);
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(client.read(&mut [0; 8]).unwrap(), 0);
    let mut local: &Stream = &server;
    assert_eq!(local.read(&mut [0; 8]).unwrap(), 0);
    assert_eq!(
        local.write(b"x").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    server.shutdown().unwrap();
}

#[test]
fn a_shutdown_leaves_what_was_written_readable() {
    let (_place, _listener, mut client, mut server) = pair("shutdown-data");
    server.write_all(b"final").unwrap();
    server.shutdown().unwrap();
    let mut received = Vec::new();
    client.read_to_end(&mut received).unwrap();
    assert_eq!(received, b"final");
}

#[test]
fn one_thread_reads_while_another_writes() {
    let (_place, _listener, client, server) = pair("duplex");
    let client = std::sync::Arc::new(client);
    client.set_read_timeout(None).unwrap();
    let reader = {
        let client = client.clone();
        thread::spawn(move || {
            let mut client = &*client;
            let mut byte = [0];
            client.read_exact(&mut byte).unwrap();
            byte[0]
        })
    };
    thread::sleep(Duration::from_millis(50));
    let mut writer: &Stream = &client;
    writer.write_all(b"a").unwrap();
    let mut server = server;
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
    server.write_all(b"b").unwrap();
    assert_eq!(reader.join().unwrap(), b'b');
}

#[test]
fn accept_timeout_ends_and_the_listener_goes_on() {
    let place = place("accept-timeout");
    let listener = Listener::bind(&place.name).unwrap();
    let started = Instant::now();
    let result = listener.accept_timeout(Some(Duration::from_millis(100)));
    assert!(matches!(result, Err(Error::TimedOut)), "{result:?}");
    assert!(started.elapsed() >= Duration::from_millis(90));

    let mut client = Stream::connect(&place.name).unwrap();
    client.write_all(b"x").unwrap();
    let mut server = listener
        .accept_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
}

#[test]
fn a_client_that_left_before_accept_does_not_end_the_listener() {
    let place = place("gone");
    let listener = Listener::bind(&place.name).unwrap();
    drop(Stream::connect(&place.name).unwrap());
    let gone = listener.accept().unwrap();
    assert_eq!((&gone).read(&mut [0; 8]).unwrap(), 0);
    let mut client = Stream::connect(&place.name).unwrap();
    client.write_all(b"x").unwrap();
    let mut server = listener.accept().unwrap();
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
}

#[test]
fn connecting_where_nothing_listens_fails_at_once() {
    let place = place("nobody");
    let started = Instant::now();
    let result = Stream::connect(&place.name);
    assert!(matches!(result, Err(Error::NotListening)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn messages_cross_a_stream() {
    let (_place, _listener, client, server) = pair("messages");
    let mut server = Messages::new(server);
    let sender = thread::spawn(move || {
        let mut client = Messages::new(client);
        client.send(b"first").unwrap();
        client.send(b"").unwrap();
        client.send(&vec![9; 300_000]).unwrap();
    });
    assert_eq!(server.recv().unwrap(), b"first");
    assert_eq!(server.recv().unwrap(), b"");
    assert_eq!(server.recv().unwrap().len(), 300_000);
    sender.join().unwrap();
}

#[test]
fn a_message_above_the_bound_is_refused_by_both_ends() {
    let (_place, _listener, client, server) = pair("oversize");
    let mut small = Messages::new(client).with_max_message(4);
    assert_eq!(
        small.send(b"too long").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut large = Messages::new(server);
    large.send(b"too long").unwrap();
    assert_eq!(small.recv().unwrap_err().kind(), io::ErrorKind::InvalidData);
}

#[test]
fn a_receive_interrupted_by_a_timeout_goes_on_over_a_real_stream() {
    let (_place, _listener, mut client, server) = pair("resume");
    server
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let mut server = Messages::new(server);
    client.write_all(b"\x00\x00\x00\x04ab").unwrap();
    assert_eq!(server.recv().unwrap_err().kind(), io::ErrorKind::TimedOut);
    client.write_all(b"cd").unwrap();
    assert_eq!(server.recv().unwrap(), b"abcd");
}

#[test]
fn each_end_reports_the_other_as_this_user_and_this_process() {
    let (_place, _listener, client, server) = pair("identity");
    let me = current_user().unwrap();
    let seen_by_server = server.peer().unwrap();
    assert_eq!(seen_by_server.user.as_deref(), Some(me.as_str()));
    assert_eq!(seen_by_server.is_current_user().unwrap(), Some(true));
    assert_eq!(seen_by_server.process, support::expected_process());
    assert_eq!(seen_by_server.host, None);

    let seen_by_client = client.peer().unwrap();
    assert_eq!(seen_by_client.user.as_deref(), Some(me.as_str()));
    assert_eq!(seen_by_client.process, support::expected_process());
}

#[test]
fn an_identity_outlives_the_peer_that_left() {
    let (_place, _listener, client, server) = pair("identity-gone");
    drop(client);
    assert!(server.peer().is_ok());
}

#[test]
fn a_meaningless_setting_is_refused_and_a_weak_one_is_not() {
    let place = place("options");
    for options in [
        Listener::options().max_instances(1),
        Listener::options().max_instances(256),
        Listener::options().socket_mode(0o1000),
        Listener::options().socket_mode(0o400),
        Listener::options().backlog(0),
        Listener::options().read_timeout(Some(Duration::ZERO)),
        Listener::options().write_timeout(Some(Duration::ZERO)),
    ] {
        assert!(matches!(
            options.bind(&place.name),
            Err(Error::InvalidOptions)
        ));
    }
    assert!(matches!(
        Stream::options()
            .timeout(Duration::ZERO)
            .connect(&place.name),
        Err(Error::InvalidOptions)
    ));
    let listener = Listener::options()
        .read_timeout(None)
        .write_timeout(None)
        .bind(&place.name)
        .unwrap();
    let names: Vec<_> = listener
        .weakened()
        .iter()
        .map(|weaker| weaker.setting.as_str())
        .collect();
    assert_eq!(names, ["read_timeout", "write_timeout"]);
}

#[test]
fn the_strict_defaults_list_nothing_as_weakened() {
    assert!(ListenerOptions::new().weakened().is_empty());
    assert!(ConnectOptions::new().weakened().is_empty());
}

#[test]
fn a_bound_too_long_for_the_clock_waits_as_none_would() {
    let (place, listener, mut client, mut server) = pair("forever");
    server.set_read_timeout(Some(Duration::MAX)).unwrap();
    client.set_write_timeout(Some(Duration::MAX)).unwrap();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        client.write_all(b"late").unwrap();
        client
    });
    let mut buffer = [0; 4];
    server.read_exact(&mut buffer).unwrap();
    assert_eq!(&buffer, b"late");
    let mut client = writer.join().unwrap();

    let mut messages = Messages::new(server);
    messages.set_read_timeout(Some(Duration::MAX)).unwrap();
    messages.set_write_timeout(Some(Duration::MAX)).unwrap();
    client.write_all(b"\x00\x00\x00\x01x").unwrap();
    assert_eq!(messages.recv().unwrap(), b"x");
    messages.send(b"y").unwrap();
    let mut frame = [0; 5];
    client.read_exact(&mut frame).unwrap();
    assert_eq!(&frame, b"\x00\x00\x00\x01y");

    let name = place.name.clone();
    let connecting = thread::spawn(move || {
        Stream::options()
            .timeout(Duration::MAX)
            .connect(&name)
            .map(drop)
    });
    listener.accept_timeout(Some(Duration::MAX)).unwrap();
    connecting.join().unwrap().unwrap();
}

#[test]
fn an_accept_with_a_timeout_ends_on_time_beside_one_that_waits() {
    let place = place("turns");
    let listener = std::sync::Arc::new(Listener::bind(&place.name).unwrap());
    let waiting = {
        let listener = listener.clone();
        thread::spawn(move || listener.accept().map(drop))
    };
    thread::sleep(Duration::from_millis(100));
    let (sender, receiver) = std::sync::mpsc::channel();
    {
        let listener = listener.clone();
        thread::spawn(move || {
            let started = Instant::now();
            let result = listener.accept_timeout(Some(Duration::from_millis(100)));
            let _ = sender.send((started.elapsed(), result.map(drop)));
        });
    }
    let (waited, result) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("accept_timeout ended within 3 s");
    assert!(matches!(result, Err(Error::TimedOut)), "{result:?}");
    assert!(waited < Duration::from_secs(2), "{waited:?}");
    let _client = Stream::connect(&place.name).unwrap();
    waiting.join().unwrap().unwrap();
}

#[test]
fn a_read_timeout_on_messages_bounds_the_whole_receive_against_a_peer_sending_a_byte_at_a_time() {
    let (_place, _listener, mut client, server) = pair("drip");
    let dripping = thread::spawn(move || {
        for byte in [0u8, 0, 0, 200] {
            client.write_all(&[byte]).unwrap();
            thread::sleep(Duration::from_millis(50));
        }
        for _ in 0..60 {
            if client.write_all(b"x").is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
    });
    let mut messages = Messages::new(server).with_max_message(1000);
    messages
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let started = Instant::now();
    assert_eq!(messages.recv().unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    drop(messages);
    dripping.join().unwrap();
}

#[test]
fn a_write_timeout_on_messages_bounds_the_whole_send_against_a_peer_reading_slowly() {
    let place = place("slow-reader");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let mut server = listener.accept().unwrap();
    let reading = thread::spawn(move || {
        let mut chunk = [0; 8192];
        for _ in 0..60 {
            if matches!(server.read(&mut chunk), Ok(0) | Err(_)) {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
    });
    let mut messages = Messages::new(client).with_max_message(64 * 1024 * 1024);
    messages
        .set_write_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let started = Instant::now();
    let error = messages.send(&vec![0; 32 * 1024 * 1024]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    drop(messages);
    reading.join().unwrap();
}
