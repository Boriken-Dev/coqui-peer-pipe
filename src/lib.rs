#![doc = include_str!("../README.md")]
#![deny(unsafe_code)]
#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

#[cfg(not(any(windows, unix)))]
compile_error!("coqui-peer-pipe needs Windows named pipes or Unix domain sockets");

mod error;
mod frame;
mod identity;
mod listener;
mod name;
mod options;
mod stream;
mod sys;

pub use coqui_peer::{MessageTransport, ReadTimeout, Weakening, WriteTimeout};
pub use error::Error;
pub use frame::{DEFAULT_MAX_MESSAGE, Messages};
pub use identity::{OsIdentity, current_user};
pub use listener::Listener;
pub use name::PipeName;
pub use options::{
    ConnectOptions, DEFAULT_BACKLOG, DEFAULT_BUFFER_SIZE, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_IO_TIMEOUT, DEFAULT_SOCKET_MODE, ListenerOptions,
};
pub use stream::Stream;
