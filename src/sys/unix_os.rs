//! The three things std and socket2 do not offer on a Unix socket.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::time::Duration;

/// The effective user of this process.
pub(crate) fn effective_user() -> u32 {
    // SAFETY: geteuid takes nothing and cannot fail.
    unsafe { libc::geteuid() }
}

/// The process, user and group the kernel recorded for the peer of `socket`.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn peer_credentials(socket: BorrowedFd<'_>) -> io::Result<(Option<u32>, u32, u32)> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: the fd is open for the borrow, and `credentials` is writable for `length` bytes.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut credentials).cast(),
            &raw mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let process = u32::try_from(credentials.pid).ok().filter(|id| *id != 0);
    Ok((process, credentials.uid, credentials.gid))
}

/// The process, user and group the kernel recorded for the peer of `socket`.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) fn peer_credentials(socket: BorrowedFd<'_>) -> io::Result<(Option<u32>, u32, u32)> {
    let mut user = 0;
    let mut group = 0;
    // SAFETY: the fd is open for the borrow, and both out-pointers are valid for the call.
    let result = unsafe { libc::getpeereid(socket.as_raw_fd(), &raw mut user, &raw mut group) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((peer_process(socket), user, group))
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn peer_process(socket: BorrowedFd<'_>) -> Option<u32> {
    let mut process: libc::pid_t = 0;
    let mut length = size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: the fd is open for the borrow, and `process` is writable for `length` bytes.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&raw mut process).cast(),
            &raw mut length,
        )
    };
    if result == 0 {
        u32::try_from(process).ok().filter(|id| *id != 0)
    } else {
        None
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
)))]
fn peer_process(_socket: BorrowedFd<'_>) -> Option<u32> {
    None
}

/// Whether `socket` became ready for `events` within `timeout`; `None` waits forever.
pub(crate) fn wait_ready(
    socket: BorrowedFd<'_>,
    events: libc::c_short,
    timeout: Option<Duration>,
) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd: socket.as_raw_fd(),
        events,
        revents: 0,
    };
    let milliseconds = timeout.map_or(-1, |limit| {
        libc::c_int::try_from(limit.as_nanos().div_ceil(1_000_000)).unwrap_or(libc::c_int::MAX)
    });
    // SAFETY: `poll` is one valid pollfd for the call.
    let ready = unsafe { libc::poll(&raw mut poll, 1, milliseconds) };
    match ready {
        -1 => Err(io::Error::last_os_error()),
        0 => Ok(false),
        _ => Ok(true),
    }
}

/// The event `wait_ready` waits on before accepting.
pub(crate) const READABLE: libc::c_short = libc::POLLIN;
/// The event `wait_ready` waits on for a connection in progress.
pub(crate) const WRITABLE: libc::c_short = libc::POLLOUT;
