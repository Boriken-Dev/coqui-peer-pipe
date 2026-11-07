//! What a listener and a connection do by default, and what can be changed.
#![forbid(unsafe_code)]

use alloc::string::ToString;
use alloc::vec::Vec;
use core::time::Duration;

use coqui_peer::Weakening;

use crate::error::Error;
use crate::listener::Listener;
use crate::name::PipeName;
use crate::stream::Stream;

/// The default bound on one read and one write of a [`Stream`].
pub const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);
/// The default bound on [`ConnectOptions::connect`].
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The default size of each direction's buffer of a pipe instance on Windows, in bytes.
pub const DEFAULT_BUFFER_SIZE: u32 = 64 * 1024;
/// The default queue of connections a socket holds for `accept` on Unix.
pub const DEFAULT_BACKLOG: i32 = 128;
/// The default mode of a socket on Unix: its owner only.
pub const DEFAULT_SOCKET_MODE: u32 = 0o600;

/// The most instances Windows lets a pipe have, which it treats as no limit of its own.
pub(crate) const UNLIMITED_INSTANCES: u32 = 255;

/// A group of a Unix socket: an id, or a name looked up when binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Group {
    Id(u32),
    Name(String),
}

impl core::fmt::Display for Group {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Id(id) => write!(f, "{id}"),
            Self::Name(name) => f.write_str(name),
        }
    }
}

/// How a [`Listener`] binds, on the strict side unless changed.
///
/// A setting for the other platform has no effect there and is not listed as a weakening. Every
/// setting looser than its default is listed by [`weakened`](Self::weakened) and logged at `debug`
/// through `tracing` when binding. The crate reports and does not judge: whether one was meant is the caller's to say.
#[derive(Clone, Debug)]
#[must_use]
pub struct ListenerOptions {
    pub(crate) remote_clients: bool,
    pub(crate) security_descriptor: Option<String>,
    pub(crate) first_instance: bool,
    pub(crate) max_instances: u32,
    pub(crate) buffer_size: u32,
    pub(crate) socket_mode: u32,
    pub(crate) socket_group: Option<Group>,
    pub(crate) shared_directory: bool,
    pub(crate) remove_stale: bool,
    pub(crate) backlog: i32,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) write_timeout: Option<Duration>,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        Self {
            remote_clients: false,
            security_descriptor: None,
            first_instance: true,
            max_instances: UNLIMITED_INSTANCES,
            buffer_size: DEFAULT_BUFFER_SIZE,
            socket_mode: DEFAULT_SOCKET_MODE,
            socket_group: None,
            shared_directory: false,
            remove_stale: true,
            backlog: DEFAULT_BACKLOG,
            read_timeout: Some(DEFAULT_IO_TIMEOUT),
            write_timeout: Some(DEFAULT_IO_TIMEOUT),
        }
    }
}

impl ListenerOptions {
    /// The strict defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Windows: accepts clients from other hosts. Off by default, by `PIPE_REJECT_REMOTE_CLIENTS`.
    pub fn allow_remote_clients(mut self, allow: bool) -> Self {
        self.remote_clients = allow;
        self
    }

    /// Windows: this descriptor, in SDDL, in place of the default, which gives the current user alone
    /// full access (`D:P(A;;GA;;;<SID>)`, with that user as owner) and, above medium integrity, labels
    /// the pipe so that nobody below this process's level can connect or add instances.
    ///
    /// This replaces all of that: no label is added, and a text with no DACL gets the token's default
    /// DACL. A descriptor granting `FILE_CREATE_PIPE_INSTANCE` (`GENERIC_WRITE` holds it) to another
    /// user lets that user add instances of this pipe and receive its clients. One holding a NUL, or not
    /// letting this user add instances, is [`Error::InvalidDescriptor`] at bind.
    pub fn security_descriptor(mut self, sddl: impl Into<String>) -> Self {
        self.security_descriptor = Some(sddl.into());
        self
    }

    /// Windows: refuses a name that already exists, by `FILE_FLAG_FIRST_PIPE_INSTANCE`. On by default.
    /// Turned off, this listener joins an existing pipe its user may add instances to, which keeps its
    /// own descriptor, and clients may reach whichever process created the other instances.
    pub fn first_instance(mut self, first: bool) -> Self {
        self.first_instance = first;
        self
    }

    /// Windows: the most instances of this pipe at once, waiting and connected, from 2 to 254; 255,
    /// the default, leaves it to the system. One instance always waits for the next client, so at
    /// `n` an [`accept`](Listener::accept) waits while `n - 1` connections are open.
    pub fn max_instances(mut self, instances: u32) -> Self {
        self.max_instances = instances;
        self
    }

    /// Windows: the size of each direction's buffer of an instance, in bytes. A write larger than
    /// the room left waits for the peer to read.
    pub fn buffer_size(mut self, bytes: u32) -> Self {
        self.buffer_size = bytes;
        self
    }

    /// Unix: the socket's mode, from the moment the path exists, any of `0` to `0o7777`. `0o600` by default; another is reported by [`weakened`](Self::weakened), never refused.
    pub fn socket_mode(mut self, mode: u32) -> Self {
        self.socket_mode = mode;
        self
    }

    /// Unix: the socket's group, by id, from the moment the path exists, so a mode with group access
    /// (`socket_mode(0o660)`) shares it with that group and no other. The process must be root or a
    /// member of the group, or the bind is [`Error::GroupDenied`]. A group other than the process's own is
    /// reported by [`weakened`](Self::weakened). On Windows it is [`Error::InvalidOptions`].
    pub fn socket_group(mut self, group: u32) -> Self {
        self.socket_group = Some(Group::Id(group));
        self
    }

    /// Unix: as [`socket_group`](Self::socket_group), the group named `name`, looked up when binding:
    /// [`Error::UnknownGroup`] when there is none.
    pub fn socket_group_name(mut self, name: impl Into<String>) -> Self {
        self.socket_group = Some(Group::Name(name.into()));
        self
    }

    /// Unix: binds even when others can write a directory on the way to the socket, or another user
    /// owns one. Off by default: such a user could replace the socket, and clients would reach theirs.
    pub fn allow_shared_directory(mut self, allow: bool) -> Self {
        self.shared_directory = allow;
        self
    }

    /// Unix: removes a socket left at the path once nothing listens on it. On by default; off, it is
    /// [`Error::NameTaken`]. A live socket, or anything that is not a socket, is never removed.
    pub fn remove_stale(mut self, remove: bool) -> Self {
        self.remove_stale = remove;
        self
    }

    /// Unix: how many connections wait for `accept`, up to the system's own limit.
    pub fn backlog(mut self, connections: i32) -> Self {
        self.backlog = connections;
        self
    }

    /// The bound on each read of an accepted [`Stream`]; `None` waits forever. The async streams
    /// have none: wrap their calls in tokio's timer.
    pub fn read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// The bound on each write of an accepted [`Stream`]; `None` waits forever.
    pub fn write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Every setting looser than its default on this platform.
    #[must_use]
    pub fn weakened(&self) -> Vec<Weakening> {
        let mut list = Vec::new();
        if cfg!(windows) {
            if self.remote_clients {
                list.push(Weakening::new("remote_clients", "allowed", "refused"));
            }
            if let Some(sddl) = &self.security_descriptor {
                list.push(Weakening::new(
                    "security_descriptor",
                    sddl.clone(),
                    "the current user only",
                ));
            }
            if !self.first_instance {
                list.push(Weakening::new("first_instance", "false", "true"));
            }
        } else {
            if self.socket_mode != DEFAULT_SOCKET_MODE {
                list.push(Weakening::new(
                    "socket_mode",
                    alloc::format!("{:o}", self.socket_mode),
                    alloc::format!("{DEFAULT_SOCKET_MODE:o}"),
                ));
            }
            #[cfg(unix)]
            if let Some(group) = &self.socket_group
                && crate::sys::group_differs(group)
            {
                list.push(Weakening::new(
                    "socket_group",
                    group.to_string(),
                    "the process's own group",
                ));
            }
            if self.shared_directory {
                list.push(Weakening::new("shared_directory", "allowed", "refused"));
            }
        }
        push_timeouts(&mut list, self.read_timeout, self.write_timeout);
        list
    }

    /// Settings changed from their default that this platform cannot honour, reported rather than dropped:
    /// the ones that are meaningless here (a Windows buffer size on Unix, a Unix backlog on Windows, a
    /// setting that only loosens a check this platform does not make). A setting that would be stricter than
    /// the default is not here: it is [`Error::InvalidOptions`] at bind.
    #[must_use]
    pub fn not_applied(&self) -> Vec<Weakening> {
        self.unsupported().1
    }

    /// Whether a setting stricter than the default cannot be honoured here, and the ones meaningless here.
    fn unsupported(&self) -> (bool, Vec<Weakening>) {
        let mut refuse = false;
        let mut ignored = Vec::new();
        if cfg!(windows) {
            refuse |= self.socket_group.is_some() || !self.remove_stale;
            if self.socket_mode != DEFAULT_SOCKET_MODE {
                ignored.push(Weakening::new(
                    "socket_mode",
                    alloc::format!("{:o}", self.socket_mode),
                    alloc::format!("{DEFAULT_SOCKET_MODE:o}"),
                ));
            }
            if self.shared_directory {
                ignored.push(Weakening::new("shared_directory", "allowed", "refused"));
            }
            if self.backlog != DEFAULT_BACKLOG {
                ignored.push(Weakening::new(
                    "backlog",
                    self.backlog.to_string(),
                    DEFAULT_BACKLOG.to_string(),
                ));
            }
        } else {
            refuse |=
                self.security_descriptor.is_some() || self.max_instances != UNLIMITED_INSTANCES;
            if self.buffer_size != DEFAULT_BUFFER_SIZE {
                ignored.push(Weakening::new(
                    "buffer_size",
                    self.buffer_size.to_string(),
                    DEFAULT_BUFFER_SIZE.to_string(),
                ));
            }
            if self.remote_clients {
                ignored.push(Weakening::new("remote_clients", "allowed", "refused"));
            }
            if !self.first_instance {
                ignored.push(Weakening::new("first_instance", "false", "true"));
            }
        }
        (refuse, ignored)
    }

    /// Binds `name`: see [`Listener::bind`].
    ///
    /// # Errors
    ///
    /// As [`Listener::bind`]; [`Error::InvalidOptions`] for a value that means nothing, and for a setting
    /// stricter than the default that this platform cannot honour: a socket group, or a stale socket kept,
    /// on Windows; a security descriptor or an instance limit on Unix.
    pub fn bind(self, name: &PipeName) -> Result<Listener, Error> {
        Listener::with_options(name, self)
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        let instances_are_valid = (2..=UNLIMITED_INSTANCES).contains(&self.max_instances);
        let mode_is_valid = self.socket_mode <= 0o7777;
        let backlog_is_valid = self.backlog > 0;
        let group_is_valid = match &self.socket_group {
            None => true,
            Some(Group::Name(name)) => cfg!(unix) && !name.is_empty() && !name.contains('\0'),
            Some(Group::Id(_)) => cfg!(unix),
        };
        if instances_are_valid
            && group_is_valid
            && !self.unsupported().0
            && mode_is_valid
            && backlog_is_valid
            && timeouts_are_valid(self.read_timeout, self.write_timeout)
        {
            Ok(())
        } else {
            Err(Error::InvalidOptions)
        }
    }

    pub(crate) fn log_weakened(&self) {
        log(&self.weakened());
        log_not_applied(&self.not_applied());
    }
}

/// How [`Stream::connect`] connects.
///
/// Every setting looser than its default is listed by [`weakened`](Self::weakened) and logged at
/// `debug` through `tracing` when connecting.
#[derive(Clone, Debug)]
#[must_use]
pub struct ConnectOptions {
    pub(crate) timeout: Duration,
    pub(crate) impersonation: bool,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) write_timeout: Option<Duration>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_CONNECT_TIMEOUT,
            impersonation: false,
            read_timeout: Some(DEFAULT_IO_TIMEOUT),
            write_timeout: Some(DEFAULT_IO_TIMEOUT),
        }
    }
}

impl ConnectOptions {
    /// The strict defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// How long to wait for a free instance or a place in the queue. A name nobody listens on fails
    /// at once. On Windows the open of another host's pipe is the system's and waits for the network,
    /// whatever this says.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Windows: lets the server act as this client. Off by default: the client connects at
    /// `SECURITY_IDENTIFICATION`, so a server, or a squatter in its place, can learn who the client is
    /// and cannot act as it.
    pub fn allow_impersonation(mut self, allow: bool) -> Self {
        self.impersonation = allow;
        self
    }

    /// The bound on each read of the [`Stream`]; `None` waits forever. The async streams have none:
    /// wrap their calls in tokio's timer.
    pub fn read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// The bound on each write of the [`Stream`]; `None` waits forever.
    pub fn write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Settings changed from their default that this platform cannot honour: `allow_impersonation` on Unix,
    /// where there is no impersonation level to raise. Reported, never dropped.
    #[must_use]
    pub fn not_applied(&self) -> Vec<Weakening> {
        let mut list = Vec::new();
        if cfg!(unix) && self.impersonation {
            list.push(Weakening::new("impersonation", "allowed", "refused"));
        }
        list
    }

    /// Every setting looser than its default on this platform.
    #[must_use]
    pub fn weakened(&self) -> Vec<Weakening> {
        let mut list = Vec::new();
        if cfg!(windows) && self.impersonation {
            list.push(Weakening::new("impersonation", "allowed", "identification"));
        }
        push_timeouts(&mut list, self.read_timeout, self.write_timeout);
        list
    }

    /// Connects to `name`: see [`Stream::connect`].
    ///
    /// # Errors
    ///
    /// As [`Stream::connect`]; [`Error::InvalidOptions`] for a zero duration.
    pub fn connect(self, name: &PipeName) -> Result<Stream, Error> {
        Stream::with_options(name, self)
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        if !self.timeout.is_zero() && timeouts_are_valid(self.read_timeout, self.write_timeout) {
            Ok(())
        } else {
            Err(Error::InvalidOptions)
        }
    }

    pub(crate) fn log_weakened(&self) {
        log(&self.weakened());
        log_not_applied(&self.not_applied());
    }
}

fn timeouts_are_valid(read: Option<Duration>, write: Option<Duration>) -> bool {
    read != Some(Duration::ZERO) && write != Some(Duration::ZERO)
}

fn push_timeouts(list: &mut Vec<Weakening>, read: Option<Duration>, write: Option<Duration>) {
    let strict = alloc::format!("{DEFAULT_IO_TIMEOUT:?}");
    if read.is_none_or(|limit| limit > DEFAULT_IO_TIMEOUT) {
        list.push(Weakening::new("read_timeout", show(read), strict.clone()));
    }
    if write.is_none_or(|limit| limit > DEFAULT_IO_TIMEOUT) {
        list.push(Weakening::new("write_timeout", show(write), strict));
    }
}

fn show(timeout: Option<Duration>) -> alloc::string::String {
    timeout.map_or_else(|| "none".to_string(), |limit| alloc::format!("{limit:?}"))
}

fn log_not_applied(list: &[Weakening]) {
    for ignored in list {
        tracing::debug!(
            setting = %ignored.setting,
            value = %ignored.value,
            default = %ignored.strict,
            "a pipe setting has no effect on this platform"
        );
    }
}

fn log(list: &[Weakening]) {
    for weaker in list {
        tracing::debug!(
            setting = %weaker.setting,
            value = %weaker.value,
            strict = %weaker.strict,
            "a pipe setting is looser than its default"
        );
    }
}
