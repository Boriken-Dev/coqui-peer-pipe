//! The crate's error.

use core::fmt;
use std::io;

/// Why a name, a bind, an accept or a connect failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name is not one this platform can use. A caller's mistake.
    InvalidName,
    /// This platform has no such thing: a Unix path on Windows, or a pipe on another host on Unix.
    Unsupported,
    /// Neither `XDG_RUNTIME_DIR` nor `TMPDIR` names an absolute directory.
    NoRuntimeDirectory,
    /// A setting is meaningless, as a socket mode above `0o777`. A caller's mistake.
    InvalidOptions,
    /// Something else holds the name: a live listener, or a pipe another process created.
    NameTaken,
    /// The path holds something other than a socket, which is never removed.
    NotASocket,
    /// A directory on the way to the socket can be written by others, so another user could replace
    /// the socket; `ListenerOptions::allow_shared_directory` accepts that.
    SharedDirectory,
    /// A directory on the way to the socket belongs to another user, who could replace the socket;
    /// `ListenerOptions::allow_shared_directory` accepts that.
    ForeignDirectory,
    /// The security descriptor text does not parse.
    InvalidDescriptor,
    /// Nothing is listening on the name.
    NotListening,
    /// The operating system refused this user or this client.
    AccessDenied,
    /// The wait ended first.
    TimedOut,
    /// Any other failure of the operating system.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidName => "the name is not valid on this platform",
            Self::Unsupported => "this platform has no such name",
            Self::NoRuntimeDirectory => "no runtime directory is set",
            Self::InvalidOptions => "a setting is not meaningful",
            Self::NameTaken => "something else holds the name",
            Self::NotASocket => "the path holds something other than a socket",
            Self::SharedDirectory => "others can write a directory on the way to the socket",
            Self::ForeignDirectory => "another user owns a directory on the way to the socket",
            Self::InvalidDescriptor => "the security descriptor does not parse",
            Self::NotListening => "nothing is listening on the name",
            Self::AccessDenied => "the operating system refused access",
            Self::TimedOut => "the wait ended first",
            Self::Io(_) => "the operating system failed",
        })
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        let kind = match &error {
            Error::Io(inner) => inner.kind(),
            Error::InvalidName | Error::InvalidOptions | Error::InvalidDescriptor => {
                io::ErrorKind::InvalidInput
            }
            Error::Unsupported => io::ErrorKind::Unsupported,
            Error::NoRuntimeDirectory => io::ErrorKind::NotFound,
            Error::NameTaken => io::ErrorKind::AddrInUse,
            Error::NotASocket => io::ErrorKind::AlreadyExists,
            Error::SharedDirectory | Error::ForeignDirectory | Error::AccessDenied => {
                io::ErrorKind::PermissionDenied
            }
            Error::NotListening => io::ErrorKind::ConnectionRefused,
            Error::TimedOut => io::ErrorKind::TimedOut,
        };
        match error {
            Error::Io(inner) => inner,
            other => Self::new(kind, other),
        }
    }
}
