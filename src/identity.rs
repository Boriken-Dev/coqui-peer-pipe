//! What the operating system says about the other end.
#![forbid(unsafe_code)]

use std::io;

/// Who the operating system says is at the other end of a stream.
///
/// This is information, for a log or a host's own policy. It never replaces a key proof: a process
/// id can be reused, and an administrator or root can act as any user.
///
/// On Unix, `process`, `user` and `group` are the credentials the kernel recorded when the socket
/// connected. A server on Windows learns the client's process and session from the pipe and its user
/// from the client's token; a client learns the server's process and session, and the user who owns
/// the pipe. Anything the system would not say is `None`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct OsIdentity {
    /// The process id. `None` for a client on another host, whose number means nothing here.
    pub process: Option<u32>,
    /// The user: a SID on Windows (`S-1-5-21-...`), a decimal uid on Unix.
    pub user: Option<String>,
    /// The primary group, a decimal gid. Unix only.
    pub group: Option<String>,
    /// The session number. Windows only.
    pub session: Option<u32>,
    /// The host a client came from, when it is another one. Windows only.
    pub host: Option<String>,
}

impl OsIdentity {
    /// Whether this is the user this process runs as, as [`current_user`] says. `None` when the
    /// system did not say who it is.
    ///
    /// # Errors
    ///
    /// When the operating system does not say who this process is.
    pub fn is_current_user(&self) -> io::Result<Option<bool>> {
        match &self.user {
            Some(user) => Ok(Some(*user == current_user()?)),
            None => Ok(None),
        }
    }
}

/// The user this thread runs as, in the form of [`OsIdentity::user`].
///
/// # Errors
///
/// When the operating system does not say.
pub fn current_user() -> io::Result<String> {
    crate::sys::current_user()
}
