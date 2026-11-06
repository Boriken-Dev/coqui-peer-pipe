//! Unix: a peer that leaves is an error to the writer, never a signal that ends its process.
//!
//! A binary of its own: these tests put SIGPIPE back to its default, as a C host leaves it, which
//! would end any other test in the process that writes to a closed socket.
#![cfg(unix)]

mod support;

use std::io::{self, Write};

use coqui_transport_pipe::{Listener, Stream};
use support::place;

fn sigpipe_at_its_default() {
    // SAFETY: setting a signal's disposition to its default takes no pointer and cannot fail here.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
}

#[test]
fn a_write_to_a_peer_that_left_is_a_broken_pipe_with_sigpipe_at_its_default() {
    sigpipe_at_its_default();
    let place = place("sigpipe");
    let listener = Listener::bind(&place.name).unwrap();
    let mut client = Stream::connect(&place.name).unwrap();
    drop(listener.accept().unwrap());
    let error = loop {
        if let Err(error) = client.write_all(b"x") {
            break error;
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn an_async_write_to_a_peer_that_left_is_a_broken_pipe_with_sigpipe_at_its_default() {
    use coqui_transport_pipe::asynch;
    use tokio::io::AsyncWriteExt;

    sigpipe_at_its_default();
    let place = place("sigpipe-async");
    let mut listener = asynch::Listener::bind(&place.name).unwrap();
    let mut client = asynch::Stream::connect(&place.name).await.unwrap();
    drop(listener.accept().await.unwrap());
    let error = loop {
        if let Err(error) = client.write_all(b"x").await {
            break error;
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}
