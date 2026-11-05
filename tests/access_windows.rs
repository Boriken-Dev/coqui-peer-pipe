//! Windows: who a pipe lets in, who may take its name, and what a client gives a server.
#![cfg(windows)]

mod support;

use core::ptr::{null, null_mut};
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::time::Duration;

use coqui_peer_pipe::{Error, Listener, PipeName, Stream, Weakening, current_user};
use support::place;
use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Security::{
    CreateRestrictedToken, DISABLE_MAX_PRIVILEGE, DuplicateToken, GetTokenInformation,
    RevertToSelf, SID_AND_ATTRIBUTES, SecurityImpersonation, TOKEN_ASSIGN_PRIMARY, TOKEN_DUPLICATE,
    TOKEN_IMPERSONATE, TOKEN_QUERY, TOKEN_USER, TokenImpersonationLevel, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_DUPLEX, ReadFile,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, ImpersonateNamedPipeClient, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken, SetThreadToken,
};

fn wide(path: &std::path::Path) -> Vec<u16> {
    OsStr::new(path)
        .encode_wide()
        .chain(core::iter::once(0))
        .collect()
}

/// A process that creates a pipe without this crate: the default descriptor, any flags.
fn raw_pipe(name: &PipeName, flags: u32) -> Option<HANDLE> {
    let path = wide(name.path());
    // SAFETY: `path` is NUL-terminated and outlives the call; null attributes mean the default.
    let handle = unsafe {
        CreateNamedPipeW(
            path.as_ptr(),
            PIPE_ACCESS_DUPLEX | flags,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
            255,
            4096,
            4096,
            0,
            null(),
        )
    };
    (handle != INVALID_HANDLE_VALUE).then_some(handle)
}

/// Runs `body` on this thread under a restricted copy of this user's token, in which the user's own
/// SID matches no allow entry: the closest thing to another account that needs no account.
fn as_restricted<T>(body: impl FnOnce() -> T) -> T {
    // SAFETY: each call gets valid out-pointers; handles are closed below, and the buffer outlives
    // the SID pointer into it.
    unsafe {
        let mut process = null_mut();
        assert_ne!(
            OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_DUPLICATE | TOKEN_QUERY | TOKEN_ASSIGN_PRIMARY | TOKEN_IMPERSONATE,
                &raw mut process,
            ),
            0
        );
        let mut buffer = vec![0u64; 128];
        let mut needed = 0;
        assert_ne!(
            GetTokenInformation(
                process,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                1024,
                &raw mut needed
            ),
            0
        );
        let user = (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid;
        let disable = SID_AND_ATTRIBUTES {
            Sid: user,
            Attributes: 0,
        };
        let mut restricted = null_mut();
        assert_ne!(
            CreateRestrictedToken(
                process,
                DISABLE_MAX_PRIVILEGE,
                1,
                &raw const disable,
                0,
                null(),
                0,
                null(),
                &raw mut restricted,
            ),
            0
        );
        let mut impersonation = null_mut();
        assert_ne!(
            DuplicateToken(restricted, SecurityImpersonation, &raw mut impersonation),
            0
        );
        assert_ne!(SetThreadToken(null(), impersonation), 0);
        let result = body();
        assert_ne!(RevertToSelf(), 0);
        CloseHandle(impersonation);
        CloseHandle(restricted);
        CloseHandle(process);
        result
    }
}

#[test]
fn another_user_is_refused_by_the_default_descriptor() {
    let place = place("restricted");
    let listener = Listener::bind(&place.name).unwrap();
    let result = as_restricted(|| Stream::connect(&place.name));
    assert!(matches!(result, Err(Error::AccessDenied)), "{result:?}");
    // This user is still let in.
    let mut client = Stream::connect(&place.name).unwrap();
    client.write_all(b"x").unwrap();
    let mut server = listener.accept().unwrap();
    let mut byte = [0];
    server.read_exact(&mut byte).unwrap();
}

#[test]
fn every_later_instance_keeps_the_descriptor() {
    let place = place("restricted-later");
    let listener = Listener::bind(&place.name).unwrap();
    let _client = Stream::connect(&place.name).unwrap();
    let _server = listener.accept().unwrap();
    let result = as_restricted(|| Stream::connect(&place.name));
    assert!(matches!(result, Err(Error::AccessDenied)), "{result:?}");
}

#[test]
fn a_descriptor_of_ones_own_says_who_else_is_let_in_and_is_listed_as_weaker() {
    let place = place("descriptor");
    let sddl = "D:(A;;GA;;;WD)";
    let listener = Listener::options()
        .security_descriptor(sddl)
        .bind(&place.name)
        .unwrap();
    assert_eq!(
        listener.weakened(),
        [Weakening::new(
            "security_descriptor",
            sddl,
            "the current user only"
        )]
    );
    let client = as_restricted(|| Stream::connect(&place.name));
    assert!(client.is_ok(), "{client:?}");
}

#[test]
fn a_descriptor_that_does_not_parse_is_refused() {
    let place = place("bad-descriptor");
    let result = Listener::options()
        .security_descriptor("not a descriptor")
        .bind(&place.name);
    assert!(
        matches!(result, Err(Error::InvalidDescriptor)),
        "{result:?}"
    );
}

#[test]
fn a_name_another_process_holds_is_refused() {
    let place = place("squatted");
    let squatter = raw_pipe(&place.name, FILE_FLAG_OVERLAPPED).unwrap();
    let result = Listener::bind(&place.name);
    assert!(matches!(result, Err(Error::NameTaken)), "{result:?}");
    // SAFETY: the handle is this test's.
    unsafe { CloseHandle(squatter) };
    assert!(Listener::bind(&place.name).is_ok());
}

#[test]
fn turning_the_first_instance_off_joins_a_pipe_and_is_listed_as_weaker() {
    let place = place("join");
    let other = raw_pipe(&place.name, FILE_FLAG_OVERLAPPED).unwrap();
    let listener = Listener::options()
        .first_instance(false)
        .bind(&place.name)
        .unwrap();
    assert_eq!(
        listener.weakened(),
        [Weakening::new("first_instance", "false", "true")]
    );
    // SAFETY: the handle is this test's.
    unsafe { CloseHandle(other) };
}

#[test]
fn a_second_listener_is_refused_with_either_case_of_the_name() {
    let place = place("second");
    let _listener = Listener::bind(&place.name).unwrap();
    assert!(matches!(Listener::bind(&place.name), Err(Error::NameTaken)));
    let upper = place.name.path().to_str().unwrap().to_ascii_uppercase();
    let upper = PipeName::at(upper).unwrap();
    assert!(matches!(Listener::bind(&upper), Err(Error::NameTaken)));
}

#[test]
fn the_name_stays_held_while_the_listener_lives_whatever_its_clients_do() {
    let place = place("held");
    let listener = Listener::bind(&place.name).unwrap();
    let squat = || {
        raw_pipe(
            &place.name,
            FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
        )
    };
    assert!(squat().is_none());
    let client = Stream::connect(&place.name).unwrap();
    assert!(squat().is_none());
    let server = listener.accept().unwrap();
    assert!(squat().is_none());
    drop(client);
    drop(server);
    assert!(squat().is_none(), "free with no connection open");
    drop(listener);
    let after = squat().expect("the name is free once the listener ended");
    // SAFETY: the handle is this test's.
    unsafe { CloseHandle(after) };
}

fn local_name_for_loopback(name: &PipeName) -> PipeName {
    let text = name.path().to_str().unwrap();
    let local = text.strip_prefix(r"\\.\pipe\").unwrap();
    PipeName::on_host("localhost", local).unwrap()
}

#[test]
fn a_client_through_file_sharing_is_refused_by_default() {
    let place = place("remote");
    let _listener = Listener::bind(&place.name).unwrap();
    let result = Stream::connect(&local_name_for_loopback(&place.name));
    assert!(matches!(result, Err(Error::AccessDenied)), "{result:?}");
}

#[test]
fn a_remote_client_allowed_by_the_builder_connects_says_where_it_is_from_and_is_listed_as_weaker() {
    let place = place("remote-allowed");
    let listener = Listener::options()
        .allow_remote_clients(true)
        .bind(&place.name)
        .unwrap();
    assert_eq!(
        listener.weakened(),
        [Weakening::new("remote_clients", "allowed", "refused")]
    );
    let client = Stream::connect(&local_name_for_loopback(&place.name)).unwrap();
    let server = listener
        .accept_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let seen = server.peer().unwrap();
    assert!(seen.host.is_some(), "{seen:?}");
    assert_eq!(
        seen.process, None,
        "another host's process number means nothing here"
    );
    drop(client);
}

/// What a raw server can do with the client: the level of the token it gets by impersonating it.
fn level_a_server_gets(allow_impersonation: bool) -> i32 {
    let place = place("level");
    let server = raw_pipe(&place.name, 0).unwrap();
    let mut client = Stream::options()
        .allow_impersonation(allow_impersonation)
        .connect(&place.name)
        .unwrap();
    client.write_all(b"x").unwrap();
    let mut byte = 0u8;
    let mut count = 0;
    let current = std::thread::spawn({
        let server = server as usize;
        move || {
            // SAFETY: the handle is open until this thread is joined; the buffer and out-pointers are
            // valid for the blocking calls, and the token handle is closed before the thread ends.
            unsafe {
                assert_ne!(
                    ReadFile(
                        server as HANDLE,
                        &raw mut byte,
                        1,
                        &raw mut count,
                        null_mut()
                    ),
                    0
                );
                assert_ne!(ImpersonateNamedPipeClient(server as HANDLE), 0);
                let mut token = null_mut();
                assert_ne!(
                    OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &raw mut token),
                    0
                );
                let mut level = 0i32;
                let mut length = 0;
                assert_ne!(
                    GetTokenInformation(
                        token,
                        TokenImpersonationLevel,
                        (&raw mut level).cast(),
                        4,
                        &raw mut length,
                    ),
                    0
                );
                CloseHandle(token);
                assert_ne!(RevertToSelf(), 0);
                level
            }
        }
    })
    .join()
    .unwrap();
    // SAFETY: the handle is this test's.
    unsafe { CloseHandle(server) };
    current
}

#[test]
fn a_server_can_identify_a_client_and_cannot_act_as_it() {
    assert_eq!(level_a_server_gets(false), 1, "SecurityIdentification");
}

#[test]
fn a_client_can_choose_to_let_a_server_act_as_it_and_lists_it_as_weaker() {
    assert_eq!(level_a_server_gets(true), 2, "SecurityImpersonation");
    let options = Stream::options().allow_impersonation(true);
    assert_eq!(options.weakened()[0].setting, "impersonation");
}

#[test]
fn a_client_learns_who_owns_a_pipe_someone_else_could_have_made() {
    let place = place("owner");
    let _listener = Listener::bind(&place.name).unwrap();
    let client = Stream::connect(&place.name).unwrap();
    let me = current_user().unwrap();
    assert_eq!(client.peer().unwrap().user.as_deref(), Some(me.as_str()));
    assert!(me.starts_with("S-1-"), "a SID");
}

#[test]
fn the_instance_limit_makes_an_accept_wait_until_a_connection_ends() {
    let place = place("instances");
    let listener = Listener::options()
        .max_instances(2)
        .bind(&place.name)
        .unwrap();
    let first = Stream::connect(&place.name).unwrap();
    let served = listener
        .accept_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let second = Stream::connect(&place.name).unwrap();
    let wait = listener.accept_timeout(Some(Duration::from_millis(100)));
    assert!(matches!(wait, Err(Error::TimedOut)), "{wait:?}");
    drop(first);
    drop(served);
    let mut served = listener
        .accept_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut second = second;
    second.write_all(b"x").unwrap();
    let mut byte = [0];
    served.read_exact(&mut byte).unwrap();
}

#[test]
fn a_client_waits_for_a_free_instance_up_to_its_timeout() {
    let place = place("busy");
    let listener = Listener::bind(&place.name).unwrap();
    let _taken = Stream::connect(&place.name).unwrap();
    let started = std::time::Instant::now();
    let result = Stream::options()
        .timeout(Duration::from_millis(150))
        .connect(&place.name);
    assert!(matches!(result, Err(Error::TimedOut)), "{result:?}");
    assert!(started.elapsed() >= Duration::from_millis(140));
    let _server = listener.accept().unwrap();
    assert!(
        Stream::connect(&place.name).is_ok(),
        "the next instance is free"
    );
}

#[test]
fn a_write_past_the_room_that_times_out_refuses_every_later_write() {
    let place = place("torn");
    let listener = Listener::options()
        .buffer_size(4096)
        .bind(&place.name)
        .unwrap();
    let mut client = Stream::connect(&place.name).unwrap();
    let mut server = listener.accept().unwrap();
    coqui_peer_pipe::WriteTimeout::set_write_timeout(&client, Some(Duration::from_millis(100)))
        .unwrap();
    let data = vec![7; 60_000];
    let error = client.write(&data).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    let error = client.write(b"x").unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    // Reading still works: only this direction's position is unknown.
    server.write_all(b"y").unwrap();
    let mut byte = [0];
    coqui_peer_pipe::ReadTimeout::set_read_timeout(&client, Some(Duration::from_secs(5))).unwrap();
    client.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"y");
}

#[test]
fn nothing_impersonated_is_left_on_the_callers_thread() {
    let place = place("thread");
    let listener = Listener::bind(&place.name).unwrap();
    let _client = Stream::connect(&place.name).unwrap();
    let server = listener.accept().unwrap();
    let _ = server.peer().unwrap();
    let mut token = null_mut();
    // SAFETY: the pseudo-handle is valid, and a call with no token fails without writing one.
    let on_thread = unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &raw mut token) };
    // SAFETY: reads this thread's value.
    let code = unsafe { GetLastError() };
    assert_eq!((on_thread, code), (0, 1008), "ERROR_NO_TOKEN");
}
