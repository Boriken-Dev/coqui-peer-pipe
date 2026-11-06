//! Helpers the test binaries share.
#![allow(dead_code)]

mod codec;
#[allow(unused_imports)]
pub use codec::LengthPrefix;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use coqui_transport_pipe::PipeName;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A name nothing else uses, and on Unix the private directory holding it.
pub struct Place {
    pub name: PipeName,
    pub directory: Option<tempfile::TempDir>,
}

/// A fresh name in a private directory, under this machine's temporary directory.
pub fn place(tag: &str) -> Place {
    let unique = NEXT.fetch_add(1, Ordering::Relaxed);
    let text = format!("t{}-{unique}-{tag}", std::process::id());
    if cfg!(windows) {
        return Place {
            name: PipeName::named(&text).unwrap(),
            directory: None,
        };
    }
    let directory = private_directory();
    let name = PipeName::named_in(directory.path(), &format!("{unique}")).unwrap();
    Place {
        name,
        directory: Some(directory),
    }
}

/// What a peer's identity says of this process: FreeBSD's `getpeereid` gives no process.
pub fn expected_process() -> Option<u32> {
    (!cfg!(target_os = "freebsd")).then(std::process::id)
}

/// A new directory only its owner can use. Kept short: a socket path has a small limit.
pub fn private_directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("p")
        .tempdir_in(temp_base())
        .unwrap()
}

/// `TMPDIR` may name a long path, so `/tmp` is used where there is one.
pub fn temp_base() -> PathBuf {
    let short = PathBuf::from("/tmp");
    if cfg!(unix) && short.is_dir() {
        short
    } else {
        std::env::temp_dir()
    }
}
