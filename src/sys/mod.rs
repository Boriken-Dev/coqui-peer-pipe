//! The operating system's side of a listener and a stream.

#[cfg(windows)]
#[allow(unsafe_code)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::*;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
#[allow(unsafe_code)]
mod unix_os;
#[cfg(unix)]
pub(crate) use unix::*;
