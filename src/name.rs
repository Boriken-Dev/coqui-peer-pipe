//! Where a listener listens: a Windows pipe name or a Unix socket path.
#![forbid(unsafe_code)]

use core::fmt;
use core::str::FromStr;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A validated name of a pipe, as the operating system will open it.
///
/// On Windows it is `\\.\pipe\NAME`, or `\\HOST\pipe\NAME` for a pipe on another host, in ASCII lower
/// case since the system compares these names without case. On Unix it is a socket path.
///
/// [`named`](Self::named) is the portable spelling; its text form is `pipe:NAME`. A path is
/// [`at`](Self::at), written `unix:PATH` on Unix. Two names are equal when they open the same thing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PipeName {
    path: PathBuf,
}

impl PipeName {
    /// The pipe called `name` for this user: `\\.\pipe\NAME` on Windows, and on Unix
    /// `NAME.sock` in the first of `XDG_RUNTIME_DIR` and `TMPDIR` that holds an absolute path.
    ///
    /// `name` is one or more of `A-Z a-z 0-9 . _ -`, and does not start with a dot. On Windows a
    /// pipe name is seen by every user of the machine, so a guessable one can be squatted by another
    /// user before this one binds it; [`Listener::bind`](crate::Listener::bind) then refuses.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidName`] for such a `name`, [`Error::NoRuntimeDirectory`] on Unix when neither
    /// variable names an absolute directory.
    pub fn named(name: &str) -> Result<Self, Error> {
        check_name(name)?;
        if cfg!(windows) {
            return Self::windows_local(name);
        }
        let directory = ["XDG_RUNTIME_DIR", "TMPDIR"]
            .iter()
            .filter_map(|variable| std::env::var_os(variable))
            .map(PathBuf::from)
            .find(|path| path.is_absolute())
            .ok_or(Error::NoRuntimeDirectory)?;
        Self::named_in(directory, name)
    }

    /// The pipe called `name`, with its socket in `directory` on Unix: `DIRECTORY/NAME.sock`.
    /// Windows pipes have no directory, so there it ignores `directory` and is
    /// [`named`](Self::named).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidName`] for a `name` as in [`named`](Self::named), or a path too long for a
    /// socket.
    pub fn named_in(directory: impl AsRef<Path>, name: &str) -> Result<Self, Error> {
        check_name(name)?;
        if cfg!(windows) {
            return Self::windows_local(name);
        }
        Self::at(directory.as_ref().join(format!("{name}.sock")))
    }

    /// The name at `path`: a socket path on Unix; on Windows `\\.\pipe\NAME` or `\\HOST\pipe\NAME`,
    /// where `NAME` may hold `\` to group names and may not hold `/`, a control character or a `.` or
    /// `..` part.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidName`] for anything else. On Unix that is an empty path, a path with no file
    /// name, or one too long for a socket.
    pub fn at(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::from_path(path.as_ref())
    }

    /// The pipe `name` on `host`, `\\HOST\pipe\NAME`, which a client reaches through the operating
    /// system's file sharing. A listener refuses such clients unless
    /// [`ListenerOptions::allow_remote_clients`](crate::ListenerOptions::allow_remote_clients).
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] on Unix; [`Error::InvalidName`] for a `host` holding anything but
    /// `A-Z a-z 0-9 . - : _`, or a `name` as in [`named`](Self::named).
    pub fn on_host(host: &str, name: &str) -> Result<Self, Error> {
        if !cfg!(windows) {
            return Err(Error::Unsupported);
        }
        check_name(name)?;
        let host_is_valid = !host.is_empty()
            && host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-:_".contains(&byte));
        if !host_is_valid {
            return Err(Error::InvalidName);
        }
        Self::at(format!(r"\\{host}\pipe\{name}"))
    }

    /// The path the operating system opens.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn windows_local(name: &str) -> Result<Self, Error> {
        Self::at(format!(r"\\.\pipe\{name}"))
    }

    #[cfg(windows)]
    fn from_path(path: &Path) -> Result<Self, Error> {
        let text = path.to_str().ok_or(Error::InvalidName)?;
        let lowered = text.to_ascii_lowercase();
        let rest = lowered.strip_prefix(r"\\").ok_or(Error::InvalidName)?;
        let (host, rest) = rest.split_once('\\').ok_or(Error::InvalidName)?;
        let name = rest.strip_prefix(r"pipe\").ok_or(Error::InvalidName)?;
        let name_is_valid = !name.is_empty()
            && name
                .chars()
                .all(|character| character != '/' && !character.is_control())
            && name
                .split('\\')
                .all(|part| !part.is_empty() && part != "." && part != "..");
        if host.is_empty() || host.contains(['/', ' ']) || !name_is_valid {
            return Err(Error::InvalidName);
        }
        Ok(Self {
            path: PathBuf::from(lowered),
        })
    }

    #[cfg(unix)]
    fn from_path(path: &Path) -> Result<Self, Error> {
        use std::os::unix::net::SocketAddr;

        if path.file_name().is_none() {
            return Err(Error::InvalidName);
        }
        // Binding goes through a private directory beside the socket, whose name adds this much.
        let mut longest = path.to_path_buf();
        longest.set_file_name(format!(".{:08x}/s", 0));
        SocketAddr::from_pathname(path).map_err(|_| Error::InvalidName)?;
        SocketAddr::from_pathname(&longest).map_err(|_| Error::InvalidName)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

fn check_name(name: &str) -> Result<(), Error> {
    let is_valid = !name.is_empty()
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
    if is_valid {
        Ok(())
    } else {
        Err(Error::InvalidName)
    }
}

impl fmt::Display for PipeName {
    /// `pipe:NAME` for a local Windows pipe, `\\HOST\pipe\NAME` for another host's, `unix:PATH` on
    /// Unix. A Unix path that is not UTF-8 is shown lossily and does not parse back.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if cfg!(windows) {
            let text = self.path.to_string_lossy();
            return match text.strip_prefix(r"\\.\pipe\") {
                Some(name) => write!(f, "pipe:{name}"),
                None => f.write_str(&text),
            };
        }
        write!(f, "unix:{}", self.path.display())
    }
}

impl FromStr for PipeName {
    type Err = Error;

    /// Takes `pipe:NAME` ([`named`](Self::named), which on Unix reads the environment), `unix:PATH`
    /// ([`at`](Self::at), [`Error::Unsupported`] on Windows), or a whole path as
    /// [`at`](Self::at) takes it: `\\` on Windows, `/` on Unix.
    fn from_str(text: &str) -> Result<Self, Error> {
        if let Some(name) = text.strip_prefix("pipe:") {
            if cfg!(windows)
                && name.contains('\\')
                && let Ok(grouped) = Self::at(format!(r"\\.\pipe\{name}"))
            {
                return Ok(grouped);
            }
            return Self::named(name);
        }
        if let Some(path) = text.strip_prefix("unix:") {
            if cfg!(windows) {
                return Err(Error::Unsupported);
            }
            return Self::at(path);
        }
        let is_whole_path = if cfg!(windows) {
            text.starts_with(r"\\")
        } else {
            text.starts_with('/')
        };
        if is_whole_path {
            Self::at(text)
        } else {
            Err(Error::InvalidName)
        }
    }
}

impl AsRef<Path> for PipeName {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
