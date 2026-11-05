//! Unix: the socket's mode, the directory around it, and what stands at its path.
#![cfg(unix)]

mod support;

use std::fs;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use coqui_peer_pipe::{Error, Listener, PipeName, Stream, Weakening, current_user};
use support::{place, private_directory, temp_base};

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

fn name_in(directory: &Path, file: &str) -> PipeName {
    PipeName::at(directory.join(file)).unwrap()
}

fn round_trip(listener: &Listener, name: &PipeName) {
    let mut client = Stream::connect(name).unwrap();
    client.write_all(b"x").unwrap();
    let mut server = listener
        .accept_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
}

#[test]
fn a_socket_is_its_owners_alone() {
    let place = place("mode");
    let _listener = Listener::bind(&place.name).unwrap();
    assert_eq!(mode(place.name.path()), 0o600);
}

#[test]
fn a_socket_is_never_seen_with_a_looser_mode() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = {
        let (path, stop) = (path.clone(), stop.clone());
        thread::spawn(move || {
            let mut seen = 0u32;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(metadata) = fs::symlink_metadata(&path) {
                    assert_eq!(metadata.mode() & 0o777, 0o600);
                    seen += 1;
                }
            }
            seen
        })
    };
    let name = PipeName::at(&path).unwrap();
    for _ in 0..200 {
        drop(Listener::bind(&name).unwrap());
    }
    stop.store(true, Ordering::Relaxed);
    assert!(watcher.join().unwrap() > 0, "the watcher saw the socket");
}

#[test]
fn nothing_of_the_bind_is_left_but_the_socket_and_its_lock() {
    let directory = private_directory();
    let name = name_in(directory.path(), "s.sock");
    let listener = Listener::bind(&name).unwrap();
    let mut left: Vec<_> = fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort();
    assert_eq!(left, ["s.sock", "s.sock.lock"]);
    assert_eq!(mode(&directory.path().join("s.sock.lock")), 0o600);
    drop(listener);
    assert!(!directory.path().join("s.sock").exists());
}

#[test]
fn a_looser_mode_is_a_choice_and_is_listed_as_weaker() {
    let place = place("mode-loose");
    let listener = Listener::options()
        .socket_mode(0o660)
        .bind(&place.name)
        .unwrap();
    assert_eq!(mode(place.name.path()), 0o660);
    assert_eq!(
        listener.weakened(),
        [Weakening::new("socket_mode", "660", "600")]
    );
}

#[test]
fn a_stale_socket_is_replaced() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    // Dropping std's listener leaves the file, as a killed process does.
    drop(UnixListener::bind(&path).unwrap());
    assert!(path.exists());
    let name = PipeName::at(&path).unwrap();
    let listener = Listener::bind(&name).unwrap();
    round_trip(&listener, &name);
}

#[test]
fn a_stale_socket_stays_when_removing_it_is_turned_off() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    drop(UnixListener::bind(&path).unwrap());
    let result = Listener::options()
        .remove_stale(false)
        .bind(&PipeName::at(&path).unwrap());
    assert!(matches!(result, Err(Error::NameTaken)), "{result:?}");
    assert!(path.exists());
}

#[test]
fn a_live_listener_of_this_crate_is_not_replaced() {
    let place = place("live");
    let listener = Listener::bind(&place.name).unwrap();
    let result = Listener::bind(&place.name);
    assert!(matches!(result, Err(Error::NameTaken)), "{result:?}");
    round_trip(&listener, &place.name);
}

#[test]
fn a_live_socket_of_another_program_is_not_replaced() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    let other = UnixListener::bind(&path).unwrap();
    let result = Listener::bind(&PipeName::at(&path).unwrap());
    assert!(matches!(result, Err(Error::NameTaken)), "{result:?}");
    let _client = UnixStream::connect(&path).unwrap();
    // The probe's connection, then this one: still the other program's socket.
    other.accept().unwrap();
    other.accept().unwrap();
}

#[test]
fn many_binders_of_one_stale_socket_leave_exactly_one_listener_reachable() {
    for _ in 0..20 {
        let directory = private_directory();
        let path = directory.path().join("s.sock");
        drop(UnixListener::bind(&path).unwrap());
        let name = PipeName::at(&path).unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let binders: Vec<_> = (0..8)
            .map(|_| {
                let (barrier, name) = (barrier.clone(), name.clone());
                thread::spawn(move || {
                    barrier.wait();
                    Listener::bind(&name)
                })
            })
            .collect();
        let results: Vec<_> = binders
            .into_iter()
            .map(|binder| binder.join().unwrap())
            .collect();
        let bound: Vec<_> = results
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .collect();
        assert_eq!(bound.len(), 1, "{results:?}");
        for result in &results {
            if let Err(error) = result {
                assert!(matches!(error, Error::NameTaken), "{error:?}");
            }
        }
        round_trip(bound[0], &name);
    }
}

#[test]
fn a_regular_file_at_the_path_is_left_alone() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    fs::write(&path, b"keep").unwrap();
    let result = Listener::bind(&PipeName::at(&path).unwrap());
    assert!(matches!(result, Err(Error::NotASocket)), "{result:?}");
    assert_eq!(fs::read(&path).unwrap(), b"keep");
}

#[test]
fn a_symbolic_link_at_the_path_is_left_alone_and_not_followed() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    let target = directory.path().join("target");
    symlink(&target, &path).unwrap();
    let result = Listener::bind(&PipeName::at(&path).unwrap());
    assert!(matches!(result, Err(Error::NotASocket)), "{result:?}");
    assert!(
        fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!target.exists());
}

#[test]
fn a_directory_others_can_write_is_refused() {
    for loose in [0o777, 0o770, 0o722, 0o1777] {
        let directory = private_directory();
        set_mode(directory.path(), loose);
        let result = Listener::bind(&name_in(directory.path(), "s.sock"));
        assert!(
            matches!(result, Err(Error::SharedDirectory)),
            "{loose:o}: {result:?}"
        );
        assert!(!directory.path().join("s.sock").exists());
    }
}

#[test]
fn a_directory_only_its_owner_writes_is_accepted_whoever_may_read_it() {
    for fine in [0o700, 0o750, 0o755] {
        let directory = private_directory();
        set_mode(directory.path(), fine);
        let name = name_in(directory.path(), "s.sock");
        let listener = Listener::bind(&name);
        assert!(listener.is_ok(), "{fine:o}: {listener:?}");
    }
}

#[test]
fn a_shared_directory_is_a_choice_and_is_listed_as_weaker() {
    let directory = private_directory();
    set_mode(directory.path(), 0o777);
    let name = name_in(directory.path(), "s.sock");
    let listener = Listener::options()
        .allow_shared_directory(true)
        .bind(&name)
        .unwrap();
    assert_eq!(
        listener.weakened(),
        [Weakening::new("shared_directory", "allowed", "refused")]
    );
    round_trip(&listener, &name);
}

#[test]
fn a_directory_above_others_can_write_is_refused_unless_sticky() {
    let directory = private_directory();
    let inner = directory.path().join("inner");
    fs::create_dir(&inner).unwrap();
    set_mode(&inner, 0o700);
    set_mode(directory.path(), 0o777);
    let result = Listener::bind(&name_in(&inner, "s.sock"));
    assert!(matches!(result, Err(Error::SharedDirectory)), "{result:?}");

    // Sticky, as /tmp is: others cannot rename what is below it.
    set_mode(directory.path(), 0o1777);
    let result = Listener::bind(&name_in(&inner, "s.sock"));
    assert!(result.is_ok(), "{result:?}");
    drop(result);
    set_mode(directory.path(), 0o700);
}

#[test]
fn a_symbolic_link_on_the_way_is_judged_by_where_it_leads() {
    let directory = private_directory();
    let shared = directory.path().join("shared");
    fs::create_dir(&shared).unwrap();
    set_mode(&shared, 0o777);
    let link = directory.path().join("link");
    symlink(&shared, &link).unwrap();
    let result = Listener::bind(&name_in(&link, "s.sock"));
    assert!(matches!(result, Err(Error::SharedDirectory)), "{result:?}");
}

#[test]
fn a_directory_that_does_not_exist_fails_as_such() {
    let directory = private_directory();
    let result = Listener::bind(&name_in(&directory.path().join("nope"), "s.sock"));
    let Err(Error::Io(error)) = result else {
        panic!("{result:?}");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn dropping_a_listener_leaves_a_socket_that_replaced_its_own() {
    let directory = private_directory();
    let path = directory.path().join("s.sock");
    let listener = Listener::bind(&PipeName::at(&path).unwrap()).unwrap();
    fs::remove_file(&path).unwrap();
    let _other = UnixListener::bind(&path).unwrap();
    drop(listener);
    assert!(path.exists(), "the path is someone else's by then");
}

#[test]
fn nothing_blocks_a_listener_after_another_was_dropped() {
    let place = place("again");
    drop(Listener::bind(&place.name).unwrap());
    let listener = Listener::bind(&place.name).unwrap();
    round_trip(&listener, &place.name);
}

#[test]
fn a_full_queue_makes_a_client_wait_up_to_its_timeout() {
    let place = place("queue");
    let _listener = Listener::options().backlog(1).bind(&place.name).unwrap();
    let mut held = Vec::new();
    let quick = Stream::options().timeout(Duration::from_millis(100));
    let error = loop {
        match quick.clone().connect(&place.name) {
            Ok(client) => held.push(client),
            Err(error) => break error,
        }
        assert!(held.len() < 4096, "the backlog bounds the queue");
    };
    // FreeBSD 14.5 refuses a connect to a full queue where Linux makes it wait.
    if cfg!(target_os = "freebsd") {
        assert!(matches!(error, Error::NotListening), "{error:?}");
    } else {
        assert!(matches!(error, Error::TimedOut), "{error:?}");
    }
}

/// Re-runs this test binary as uid 65534, from a copy any user can execute, connecting to `path`:
/// its answer is what another user gets.
fn connect_as_another_user(path: &Path) -> String {
    use std::os::unix::process::CommandExt;

    let shared = tempfile::Builder::new()
        .prefix("x")
        .tempdir_in(temp_base())
        .unwrap();
    set_mode(shared.path(), 0o755);
    let copy = shared.path().join("t");
    fs::copy(std::env::current_exe().unwrap(), &copy).unwrap();
    set_mode(&copy, 0o755);
    let output = Command::new(&copy)
        .args(["--exact", "prints_what_connecting_gives", "--nocapture"])
        .env("COQUI_PEER_PIPE_TEST_CONNECT", path.as_os_str())
        .uid(65534)
        .gid(65534)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("CONNECT=").map(str::to_owned))
        .unwrap_or_default()
}

#[test]
fn prints_what_connecting_gives() {
    if let Some(path) = std::env::var_os("COQUI_PEER_PIPE_TEST_CONNECT") {
        let name = PipeName::at(PathBuf::from(path)).unwrap();
        match Stream::connect(&name) {
            Ok(mut client) => {
                println!("CONNECT=connected");
                // Only for a server that answers; one that does not has already failed its test.
                let _ = client.write_all(b"x");
                let _ = client.read_exact(&mut [0]);
            }
            Err(Error::AccessDenied) => println!("CONNECT=denied"),
            Err(other) => println!("CONNECT={other:?}"),
        }
    }
}

/// A directory another user can pass through, so a refusal is the socket's.
fn searchable_directory() -> tempfile::TempDir {
    let directory = tempfile::Builder::new()
        .prefix("s")
        .tempdir_in(temp_base())
        .unwrap();
    set_mode(directory.path(), 0o755);
    directory
}

#[test]
#[ignore = "needs root, to run a client as another user"]
fn another_user_is_refused_by_the_sockets_mode() {
    assert_eq!(current_user().unwrap(), "0", "run this one as root");
    // With no mask to help it, only the bind's own mode can keep the other user out.
    // SAFETY: umask takes a number and cannot fail. The root tests run on one thread.
    unsafe { libc::umask(0) };
    let directory = searchable_directory();
    let name = name_in(directory.path(), "s.sock");
    let listener = Listener::bind(&name).unwrap();
    assert_eq!(connect_as_another_user(name.path()), "denied");
    assert!(matches!(
        listener.accept_timeout(Some(Duration::from_millis(100))),
        Err(Error::TimedOut)
    ));
}

#[test]
#[ignore = "needs root, to run a client as another user"]
fn another_user_let_in_by_a_looser_mode_is_reported_by_the_kernel() {
    assert_eq!(current_user().unwrap(), "0", "run this one as root");
    let directory = searchable_directory();
    let name = name_in(directory.path(), "s.sock");
    let listener = Listener::options().socket_mode(0o666).bind(&name).unwrap();
    let path = name.path().to_path_buf();
    let client = thread::spawn(move || connect_as_another_user(&path));
    let mut server = listener
        .accept_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let seen = server.peer().unwrap();
    assert_eq!(seen.user.as_deref(), Some("65534"));
    assert_eq!(seen.group.as_deref(), Some("65534"));
    assert_eq!(seen.is_current_user().unwrap(), Some(false));
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
    server.write_all(&byte).unwrap();
    assert_eq!(client.join().unwrap(), "connected");
}

#[test]
#[ignore = "needs root, to give a directory to another user"]
fn a_directory_of_another_user_is_refused() {
    assert_eq!(current_user().unwrap(), "0", "run this one as root");
    let directory = searchable_directory();
    let c_path = std::ffi::CString::new(directory.path().as_os_str().as_bytes()).unwrap();
    // SAFETY: the path is NUL-terminated and outlives the call.
    assert_eq!(unsafe { libc::chown(c_path.as_ptr(), 65534, 65534) }, 0);
    let result = Listener::bind(&name_in(directory.path(), "s.sock"));
    assert!(matches!(result, Err(Error::ForeignDirectory)), "{result:?}");
    let allowed = Listener::options()
        .allow_shared_directory(true)
        .bind(&name_in(directory.path(), "s.sock"));
    assert!(allowed.is_ok(), "{allowed:?}");
}

#[test]
#[ignore = "needs root, to leave a socket owned by another user"]
fn a_stale_socket_of_another_user_is_left_alone() {
    assert_eq!(current_user().unwrap(), "0", "run this one as root");
    let directory = searchable_directory();
    let path = directory.path().join("s.sock");
    drop(UnixListener::bind(&path).unwrap());
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: the path is NUL-terminated and outlives the call.
    assert_eq!(unsafe { libc::chown(c_path.as_ptr(), 65534, 65534) }, 0);
    let result = Listener::bind(&PipeName::at(&path).unwrap());
    assert!(matches!(result, Err(Error::NameTaken)), "{result:?}");
    assert!(path.exists());
}
