//! A name and settings to connect to, as a `coqui_transport::Connector`, and the listener as a
//! `coqui_transport::Listener`.
#![forbid(unsafe_code)]

use core::time::Duration;

use crate::error::Error;
use crate::listener::Listener;
use crate::name::PipeName;
use crate::options::ConnectOptions;
use crate::stream::Stream;

/// Connects to one [`PipeName`] with [`ConnectOptions`].
///
/// As a [`coqui_transport::Connector`], `connect`'s timeout replaces the options' own, and nothing
/// proves who listens: see [`Stream::connect`].
#[derive(Clone, Debug)]
#[must_use]
pub struct Connector {
    pub(crate) name: PipeName,
    pub(crate) options: ConnectOptions,
}

impl Connector {
    /// A connector for `name` with [`ConnectOptions`]'s defaults.
    pub fn new(name: PipeName) -> Self {
        Self {
            name,
            options: ConnectOptions::new(),
        }
    }

    /// The settings to connect with.
    pub fn with_options(mut self, options: ConnectOptions) -> Self {
        self.options = options;
        self
    }

    /// The name this connects to.
    #[must_use]
    pub fn name(&self) -> &PipeName {
        &self.name
    }
}

impl coqui_transport::Connector for Connector {
    type Connection = Stream;
    type Error = Error;

    fn connect(&self, timeout: Duration) -> Result<Stream, Error> {
        Stream::with_options(&self.name, self.options.clone().timeout(timeout))
    }
}

impl coqui_transport::Listener for Listener {
    type Connection = Stream;
    type Error = Error;

    fn accept(&self) -> Result<Stream, Error> {
        Self::accept(self)
    }
}
