//! A listener, on either platform.
#![forbid(unsafe_code)]

use alloc::vec::Vec;
use core::time::Duration;

use coqui_peer::Weakening;

use crate::error::Error;
use crate::name::PipeName;
use crate::options::ListenerOptions;
use crate::stream::Stream;
use crate::sys;

/// Listens on a name and accepts connections, with the operating system's access control on.
///
/// On Windows the pipe's descriptor gives the current user alone access, the bind fails if the name
/// exists, and clients on other hosts are refused. On Unix the socket is `0600` from the moment its path
/// exists, the directories on its way can be written only by its owner, and a socket left behind is
/// removed only when nothing listens on it. Each can be loosened on [`ListenerOptions`], and is then
/// listed and logged.
///
/// Dropping it ends listening. A Unix socket's path goes with it; its `<path>.lock` stays.
#[derive(Debug)]
pub struct Listener {
    inner: sys::Listener,
    name: PipeName,
    weakened: Vec<Weakening>,
}

impl Listener {
    /// Binds `name` with [`ListenerOptions`]'s strict defaults.
    ///
    /// # Errors
    ///
    /// [`Error::NameTaken`] when something else holds the name. On Unix [`Error::NotASocket`],
    /// [`Error::SharedDirectory`], [`Error::ForeignDirectory`]. [`Error::InvalidName`] for a name the
    /// system refuses, [`Error::Io`] for the rest.
    pub fn bind(name: &PipeName) -> Result<Self, Error> {
        Self::with_options(name, ListenerOptions::new())
    }

    /// Settings for a bind.
    pub fn options() -> ListenerOptions {
        ListenerOptions::new()
    }

    pub(crate) fn with_options(name: &PipeName, options: ListenerOptions) -> Result<Self, Error> {
        options.check()?;
        options.log_weakened();
        let weakened = options.weakened();
        Ok(Self {
            inner: sys::Listener::bind(name, &options)?,
            name: name.clone(),
            weakened,
        })
    }

    /// Waits for the next connection.
    ///
    /// # Errors
    ///
    /// When the operating system fails. [`Error::Io`], or on Windows [`Error::InvalidOptions`] never: the
    /// wait has no bound.
    pub fn accept(&self) -> Result<Stream, Error> {
        self.accept_timeout(None)
    }

    /// Waits for the next connection for at most `timeout`; `None` waits forever. A wait that ends
    /// loses nothing, and the listener goes on. On Windows callers on one listener wait in turn.
    ///
    /// # Errors
    ///
    /// [`Error::TimedOut`], or [`Error::Io`].
    pub fn accept_timeout(&self, timeout: Option<Duration>) -> Result<Stream, Error> {
        Ok(Stream::from_sys(self.inner.accept(timeout)?))
    }

    /// The name this listens on.
    #[must_use]
    pub fn name(&self) -> &PipeName {
        &self.name
    }

    /// Every setting this was bound with that is looser than its default.
    #[must_use]
    pub fn weakened(&self) -> &[Weakening] {
        &self.weakened
    }
}
