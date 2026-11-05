//! Windows named pipes.
//!
//! Every handle is opened overlapped, so a wait can end on a timeout or on `shutdown`. The overlapped
//! record and the buffer of an operation stay in place until `complete` has seen it finish, which it
//! always waits for, after cancelling if need be.

use core::ptr::{null, null_mut};
use core::time::Duration;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::Instant;

use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_BROKEN_PIPE, ERROR_FILE_NOT_FOUND, ERROR_FILENAME_EXCED_RANGE,
    ERROR_INVALID_NAME, ERROR_IO_PENDING, ERROR_NO_DATA, ERROR_OPERATION_ABORTED,
    ERROR_PATH_NOT_FOUND, ERROR_PIPE_BUSY, ERROR_PIPE_CONNECTED, ERROR_PIPE_LOCAL,
    ERROR_PIPE_NOT_CONNECTED, ERROR_SEM_TIMEOUT, GENERIC_READ, GENERIC_WRITE, GetLastError, HANDLE,
    INVALID_HANDLE_VALUE, LocalFree, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, RevertToSelf,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
    PIPE_ACCESS_DUPLEX, ReadFile, SECURITY_IDENTIFICATION, SECURITY_IMPERSONATION,
    SECURITY_SQOS_PRESENT, WriteFile,
};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, GetNamedPipeClientComputerNameW,
    GetNamedPipeClientProcessId, GetNamedPipeClientSessionId, GetNamedPipeServerProcessId,
    GetNamedPipeServerSessionId, ImpersonateNamedPipeClient, PIPE_READMODE_BYTE,
    PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT, WaitNamedPipeW,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, GetCurrentProcess, GetCurrentThread, INFINITE, OpenProcessToken, OpenThreadToken,
    SetEvent, WaitForMultipleObjects,
};

use crate::error::Error;
use crate::identity::OsIdentity;
use crate::name::PipeName;
use crate::options::{ConnectOptions, ListenerOptions};

/// One `WriteFile` at most, so a timed wait measures the peer's progress, not the size of a message.
const MAX_WRITE: usize = 64 * 1024;
/// How often a bounded wait for a free instance looks again.
const BUSY_POLL: Duration = Duration::from_millis(10);

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(core::iter::once(0)).collect()
}

fn path_of(name: &PipeName) -> Result<Vec<u16>, Error> {
    Ok(wide(name.path().to_str().ok_or(Error::InvalidName)?))
}

fn last_error() -> u32 {
    // SAFETY: GetLastError takes nothing and reads this thread's value.
    unsafe { GetLastError() }
}

fn os_error(code: u32) -> io::Error {
    io::Error::from_raw_os_error(code as i32)
}

/// Takes a handle a successful call returned.
fn owned(handle: HANDLE) -> OwnedHandle {
    // SAFETY: every caller passes a valid handle it just received and owns nothing else of it.
    unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) }
}

/// A manual-reset event, not signalled.
fn new_event() -> io::Result<OwnedHandle> {
    // SAFETY: null attributes and a null name are allowed: an unnamed event with default security.
    let event = unsafe { CreateEventW(null(), 1, 0, null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    Ok(owned(event))
}

/// How a started operation ended.
enum Completion {
    Done(u32),
    Failed(u32),
    TimedOut,
    Closing,
}

/// Why `wait` returned.
enum Woken {
    Completed,
    TimedOut,
    Closing,
}

fn wait(event: RawHandle, closing: Option<RawHandle>, timeout: Option<Duration>) -> Woken {
    let handles = [event as HANDLE, closing.unwrap_or(event) as HANDLE];
    let deadline = timeout.map(|limit| Instant::now() + limit);
    loop {
        // Rounded up, so a wait never ends before its time; held below INFINITE, and looped for longer.
        let milliseconds = deadline.map_or(INFINITE, |until| {
            let left = until.saturating_duration_since(Instant::now());
            u32::try_from(left.as_nanos().div_ceil(1_000_000)).unwrap_or(INFINITE - 1)
        });
        let count = if closing.is_some() { 2 } else { 1 };
        // SAFETY: both handles are open for this call. The first one signalled wins, so a finished
        // operation is seen before a shutdown.
        let woken = unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, milliseconds) };
        if woken == WAIT_OBJECT_0 {
            return Woken::Completed;
        }
        if woken == WAIT_OBJECT_0 + 1 {
            return Woken::Closing;
        }
        if woken == WAIT_TIMEOUT {
            if deadline.is_some_and(|until| Instant::now() < until) {
                continue;
            }
            return Woken::TimedOut;
        }
        // A wait that failed ends like a timeout: `complete` cancels and waits for the operation.
        return Woken::TimedOut;
    }
}

/// Waits for an operation started with `overlapped` to end, cancelling it on a timeout or a shutdown.
///
/// # Safety
///
/// `overlapped` is the record of an operation on `handle`, started with `started` as its result and
/// `overlapped.hEvent` as its event. The caller keeps the record and the operation's buffer in place
/// until this returns, which is when the operating system no longer uses them.
unsafe fn complete(
    handle: RawHandle,
    started: i32,
    overlapped: &OVERLAPPED,
    closing: Option<RawHandle>,
    timeout: Option<Duration>,
) -> Completion {
    if started == 0 {
        let code = last_error();
        if code != ERROR_IO_PENDING {
            return Completion::Failed(code);
        }
    }
    let interrupted = match wait(overlapped.hEvent as RawHandle, closing, timeout) {
        Woken::Completed => None,
        Woken::TimedOut => Some(Completion::TimedOut),
        Woken::Closing => Some(Completion::Closing),
    };
    if interrupted.is_some() {
        // SAFETY: the record belongs to this handle. It fails only for an operation already finished.
        unsafe { CancelIoEx(handle as HANDLE, overlapped) };
    }
    let mut transferred = 0;
    // SAFETY: the record is in place, and waiting (the last argument) returns only once the
    // operation has ended, cancelled or not.
    let finished =
        unsafe { GetOverlappedResult(handle as HANDLE, overlapped, &raw mut transferred, 1) };
    if finished != 0 {
        return Completion::Done(transferred);
    }
    let code = last_error();
    match (code, interrupted) {
        (ERROR_OPERATION_ABORTED, Some(reason)) => reason,
        _ => Completion::Failed(code),
    }
}

fn new_overlapped(event: &OwnedHandle) -> OVERLAPPED {
    // SAFETY: OVERLAPPED is plain data for which all zeroes is the documented start.
    let mut overlapped: OVERLAPPED = unsafe { core::mem::zeroed() };
    overlapped.hEvent = event.as_raw_handle() as HANDLE;
    overlapped
}

#[derive(Debug)]
pub(crate) struct Stream {
    /// Held shared for each operation and taken whole to close, so a close waits for them.
    handle: RwLock<Option<OwnedHandle>>,
    closing: OwnedHandle,
    server: bool,
    read_timeout: Mutex<Option<Duration>>,
    write_timeout: Mutex<Option<Duration>>,
    /// A write timed out, and how much of it the peer took cannot be known.
    write_torn: Mutex<bool>,
}

impl Stream {
    pub(crate) fn new(
        handle: OwnedHandle,
        server: bool,
        read: Option<Duration>,
        write: Option<Duration>,
    ) -> io::Result<Self> {
        Ok(Self {
            handle: RwLock::new(Some(handle)),
            closing: new_event()?,
            server,
            read_timeout: Mutex::new(read),
            write_timeout: Mutex::new(write),
            write_torn: Mutex::new(false),
        })
    }

    pub(crate) fn connect(name: &PipeName, options: &ConnectOptions) -> Result<Self, Error> {
        let path = path_of(name)?;
        let deadline = Instant::now() + options.timeout;
        loop {
            if let Some(handle) = open(&path, options.impersonation)? {
                return Ok(Self::new(
                    handle,
                    false,
                    options.read_timeout,
                    options.write_timeout,
                )?);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::TimedOut);
            }
            // Zero asks for the pipe's default wait, so at least one millisecond is asked for.
            let milliseconds = u32::try_from(left.as_nanos().div_ceil(1_000_000))
                .unwrap_or(INFINITE - 1)
                .max(1);
            // SAFETY: `path` is NUL-terminated and outlives the call.
            let waited = unsafe { WaitNamedPipeW(path.as_ptr(), milliseconds) };
            if waited == 0 {
                match last_error() {
                    ERROR_SEM_TIMEOUT => return Err(Error::TimedOut),
                    ERROR_FILE_NOT_FOUND => return Err(Error::NotListening),
                    code => return Err(Error::Io(os_error(code))),
                }
            }
        }
    }

    pub(crate) fn read(&self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let timeout = *self
            .read_timeout
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = self.handle.read().unwrap_or_else(PoisonError::into_inner);
        let Some(handle) = held.as_ref() else {
            return Ok(0);
        };
        let handle = handle.as_raw_handle();
        let event = new_event()?;
        let mut overlapped = new_overlapped(&event);
        let length = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
        // SAFETY: `buffer` is writable for `length` bytes, and it and `overlapped` stay in place until
        // `complete` returns below, in this frame.
        let started = unsafe {
            ReadFile(
                handle as HANDLE,
                buffer.as_mut_ptr(),
                length,
                null_mut(),
                &raw mut overlapped,
            )
        };
        // SAFETY: as above; `overlapped` belongs to the operation just started on `handle`.
        let ended = unsafe {
            complete(
                handle,
                started,
                &overlapped,
                Some(self.closing.as_raw_handle()),
                timeout,
            )
        };
        match ended {
            Completion::Done(count) => Ok(count as usize),
            // The peer closed, after everything it wrote was read.
            Completion::Failed(ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED) => Ok(0),
            Completion::Failed(code) => Err(os_error(code)),
            Completion::TimedOut => Err(io::ErrorKind::TimedOut.into()),
            Completion::Closing => Ok(0),
        }
    }

    pub(crate) fn write(&self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if *self
            .write_torn
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "an earlier write timed out, and how much of it arrived cannot be known",
            ));
        }
        let timeout = *self
            .write_timeout
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let held = self.handle.read().unwrap_or_else(PoisonError::into_inner);
        let Some(handle) = held.as_ref() else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        let handle = handle.as_raw_handle();
        let event = new_event()?;
        let mut overlapped = new_overlapped(&event);
        let length = buffer.len().min(MAX_WRITE) as u32;
        // SAFETY: `buffer` is readable for `length` bytes, and it and `overlapped` stay in place until
        // `complete` returns below, in this frame.
        let started = unsafe {
            WriteFile(
                handle as HANDLE,
                buffer.as_ptr(),
                length,
                null_mut(),
                &raw mut overlapped,
            )
        };
        // SAFETY: as above; `overlapped` belongs to the operation just started on `handle`.
        let ended = unsafe {
            complete(
                handle,
                started,
                &overlapped,
                Some(self.closing.as_raw_handle()),
                timeout,
            )
        };
        match ended {
            Completion::Done(count) => Ok(count as usize),
            Completion::Failed(ERROR_NO_DATA | ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED) => {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            Completion::Failed(code) => Err(os_error(code)),
            Completion::TimedOut => {
                *self
                    .write_torn
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = true;
                Err(io::ErrorKind::TimedOut.into())
            }
            Completion::Closing => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        check_timeout(timeout)?;
        *self
            .read_timeout
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = timeout;
        Ok(())
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        check_timeout(timeout)?;
        *self
            .write_timeout
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = timeout;
        Ok(())
    }

    pub(crate) fn peer(&self) -> io::Result<OsIdentity> {
        let held = self.handle.read().unwrap_or_else(PoisonError::into_inner);
        let handle = held
            .as_ref()
            .ok_or(io::ErrorKind::NotConnected)?
            .as_raw_handle();
        Ok(peer_of(handle, self.server))
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        // SAFETY: the event is open for as long as `self`.
        if unsafe { SetEvent(self.closing.as_raw_handle() as HANDLE) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // Closing leaves what was written readable by the peer, as disconnecting would not.
        *self.handle.write().unwrap_or_else(PoisonError::into_inner) = None;
        Ok(())
    }
}

/// std refuses a zero timeout on a socket, and a pipe has the same contract.
fn check_timeout(timeout: Option<Duration>) -> io::Result<()> {
    if timeout == Some(Duration::ZERO) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a timeout of zero is not a bound",
        ));
    }
    Ok(())
}

/// One attempt to open the client's end: the handle, or `None` when every instance is in use.
pub(crate) fn open(path: &[u16], impersonation: bool) -> Result<Option<OwnedHandle>, Error> {
    let level = if impersonation {
        SECURITY_IMPERSONATION
    } else {
        SECURITY_IDENTIFICATION
    };
    // SAFETY: `path` is NUL-terminated and outlives the call; the null security attributes and
    // template are allowed.
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            0,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED | SECURITY_SQOS_PRESENT | level,
            null_mut(),
        )
    };
    if handle != INVALID_HANDLE_VALUE {
        return Ok(Some(owned(handle)));
    }
    match last_error() {
        ERROR_PIPE_BUSY => Ok(None),
        ERROR_FILE_NOT_FOUND | ERROR_PATH_NOT_FOUND => Err(Error::NotListening),
        ERROR_ACCESS_DENIED => Err(Error::AccessDenied),
        code => Err(Error::Io(os_error(code))),
    }
}

/// What every instance of one pipe is created with.
#[derive(Debug)]
pub(crate) struct Instances {
    path: Vec<u16>,
    remote_clients: bool,
    max_instances: u32,
    buffer_size: u32,
    first_instance: bool,
    descriptor: Option<Vec<u16>>,
}

impl Instances {
    pub(crate) fn new(name: &PipeName, options: &ListenerOptions) -> Result<Self, Error> {
        Ok(Self {
            path: path_of(name)?,
            remote_clients: options.remote_clients,
            max_instances: options.max_instances,
            buffer_size: options.buffer_size,
            first_instance: options.first_instance,
            descriptor: options
                .security_descriptor
                .as_deref()
                .map(wide),
        })
    }

    /// Creates the instance that takes the name, or one more of a pipe this holds. `Ok(None)` is an
    /// instance limit reached for now.
    pub(crate) fn create(&self, first: bool) -> Result<Option<OwnedHandle>, Error> {
        let mut open_mode = PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED;
        let _ = first;
        let _ = FILE_FLAG_FIRST_PIPE_INSTANCE;
        let mut pipe_mode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT;
        let _ = PIPE_REJECT_REMOTE_CLIENTS;
        let _ = &mut open_mode;
        let _ = &mut pipe_mode;
        // SAFETY: `path` is NUL-terminated and outlives the call; null attributes mean the default.
        let handle = unsafe {
            CreateNamedPipeW(
                self.path.as_ptr(),
                open_mode,
                pipe_mode,
                self.max_instances,
                self.buffer_size,
                self.buffer_size,
                0,
                null(),
            )
        };
        if handle != INVALID_HANDLE_VALUE {
            return Ok(Some(owned(handle)));
        }
        match (last_error(), first) {
            (ERROR_PIPE_BUSY, false) => Ok(None),
            (ERROR_ACCESS_DENIED | ERROR_PIPE_BUSY, true) => Err(Error::NameTaken),
            (ERROR_INVALID_NAME | ERROR_PATH_NOT_FOUND | ERROR_FILENAME_EXCED_RANGE, _) => {
                Err(Error::InvalidName)
            }
            (code, _) => Err(Error::Io(os_error(code))),
        }
    }
}

#[derive(Debug)]
struct Waiting {
    /// The instance the next client connects to. There is always one, so the name stays held.
    pending: OwnedHandle,
    /// The instance that follows, made before a client is taken: a client is never dropped for
    /// want of it, and the name is never free between two instances.
    spare: Option<OwnedHandle>,
}

#[derive(Debug)]
pub(crate) struct Listener {
    instances: Instances,
    waiting: Mutex<Waiting>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl Listener {
    pub(crate) fn bind(name: &PipeName, options: &ListenerOptions) -> Result<Self, Error> {
        let instances = Instances::new(name, options)?;
        let pending = instances.create(true)?.ok_or(Error::NameTaken)?;
        Ok(Self {
            instances,
            waiting: Mutex::new(Waiting {
                pending,
                spare: None,
            }),
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
        })
    }

    pub(crate) fn accept(&self, timeout: Option<Duration>) -> Result<Stream, Error> {
        let deadline = timeout.map(|limit| Instant::now() + limit);
        let mut waiting = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        while waiting.spare.is_none() {
            waiting.spare = self.instances.create(false)?;
            if waiting.spare.is_none() {
                if deadline.is_some_and(|until| Instant::now() >= until) {
                    return Err(Error::TimedOut);
                }
                std::thread::sleep(BUSY_POLL);
            }
        }
        wait_for_client(&waiting.pending, deadline)?;
        let next = waiting
            .spare
            .take()
            .expect("the loop above leaves a spare instance");
        let connected = core::mem::replace(&mut waiting.pending, next);
        Ok(Stream::new(
            connected,
            true,
            self.read_timeout,
            self.write_timeout,
        )?)
    }
}

fn wait_for_client(instance: &OwnedHandle, deadline: Option<Instant>) -> Result<(), Error> {
    let handle = instance.as_raw_handle();
    let event = new_event()?;
    let mut overlapped = new_overlapped(&event);
    // SAFETY: `overlapped` stays in place until `complete` returns below, in this frame.
    let started = unsafe { ConnectNamedPipe(handle as HANDLE, &raw mut overlapped) };
    if started == 0 && last_error() == ERROR_PIPE_CONNECTED {
        return Ok(());
    }
    let left = deadline.map(|until| until.saturating_duration_since(Instant::now()));
    // SAFETY: as above; `overlapped` belongs to the operation just started on `handle`.
    match unsafe { complete(handle, started, &overlapped, None, left) } {
        Completion::Done(_) => Ok(()),
        // A client that connected and left before this call: it is seen by the read that follows.
        Completion::Failed(ERROR_NO_DATA) => Ok(()),
        Completion::Failed(code) => Err(Error::Io(os_error(code))),
        Completion::TimedOut | Completion::Closing => Err(Error::TimedOut),
    }
}

fn token_user(token: HANDLE) -> io::Result<String> {
    let mut needed = 0;
    // SAFETY: a null buffer of size zero asks only for the size.
    unsafe { GetTokenInformation(token, TokenUser, null_mut(), 0, &raw mut needed) };
    // u64s hold TOKEN_USER's alignment.
    let mut buffer = vec![0u64; (needed as usize).div_ceil(8)];
    // SAFETY: `buffer` is writable for `needed` bytes.
    let got = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            needed,
            &raw mut needed,
        )
    };
    if got == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call succeeded, so the buffer starts with an initialized, aligned TOKEN_USER whose
    // SID points inside the buffer.
    let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    sid_text(sid)
}

fn sid_text(sid: PSID) -> io::Result<String> {
    let mut text: *mut u16 = null_mut();
    // SAFETY: the caller's `sid` is valid; `text` receives a LocalAlloc'd string on success.
    if unsafe { ConvertSidToStringSidW(sid, &raw mut text) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut length = 0;
    // SAFETY: the string is NUL-terminated, so every unit read up to the NUL is inside it.
    while unsafe { *text.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: `length` units were just read from it.
    let result = String::from_utf16_lossy(unsafe { core::slice::from_raw_parts(text, length) });
    // SAFETY: the string came from the call above and is not used after this.
    unsafe { LocalFree(text.cast()) };
    Ok(result)
}

pub(crate) fn current_user() -> io::Result<String> {
    let mut token = null_mut();
    // SAFETY: the pseudo-handle is always valid; `token` receives a handle on success.
    let on_thread = unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &raw mut token) };
    if on_thread == 0 {
        // SAFETY: as above.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let token = owned(token);
    token_user(token.as_raw_handle() as HANDLE)
}

/// The user of the client of `server`, read from its token on a thread of its own: an impersonating
/// thread acts as the client, and a thread that ends cannot go on doing so.
fn client_user(server: RawHandle) -> Option<String> {
    let handle = server as usize;
    std::thread::scope(|scope| {
        scope
            .spawn(move || -> Option<String> {
                // SAFETY: the caller holds the handle open across this scope.
                if unsafe { ImpersonateNamedPipeClient(handle as HANDLE) } == 0 {
                    return None;
                }
                let mut token = null_mut();
                // SAFETY: the pseudo-handle is valid; opened as this process, since the client's token
                // may be too weak to open itself.
                let opened =
                    unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &raw mut token) };
                let user = (opened != 0).then(|| {
                    let token = owned(token);
                    token_user(token.as_raw_handle() as HANDLE).ok()
                });
                // SAFETY: takes nothing; ends the impersonation on this thread.
                unsafe { RevertToSelf() };
                user.flatten()
            })
            .join()
            .ok()
            .flatten()
    })
}

/// The owner of the pipe object, who created it.
fn pipe_owner(pipe: RawHandle) -> Option<String> {
    let mut owner: PSID = null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
    // SAFETY: the handle is open, and the out-pointers are valid for the call.
    let result = unsafe {
        GetSecurityInfo(
            pipe as HANDLE,
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &raw mut owner,
            null_mut(),
            null_mut(),
            null_mut(),
            &raw mut descriptor,
        )
    };
    if result != 0 {
        return None;
    }
    let text = sid_text(owner).ok();
    // SAFETY: the descriptor came from the call above and `owner`, which points into it, is no longer used.
    unsafe { LocalFree(descriptor.cast()) };
    text
}

pub(crate) fn peer_of(handle: RawHandle, server: bool) -> OsIdentity {
    let raw = handle as HANDLE;
    let mut process = 0;
    let mut session = 0;
    let (got_process, got_session);
    if server {
        // SAFETY: the handle is open, and the out-pointers are valid for each call.
        got_process = unsafe { GetNamedPipeClientProcessId(raw, &raw mut process) } != 0;
        // SAFETY: as above.
        got_session = unsafe { GetNamedPipeClientSessionId(raw, &raw mut session) } != 0;
    } else {
        // SAFETY: as above.
        got_process = unsafe { GetNamedPipeServerProcessId(raw, &raw mut process) } != 0;
        // SAFETY: as above.
        got_session = unsafe { GetNamedPipeServerSessionId(raw, &raw mut session) } != 0;
    }
    let host = if server { client_host(raw) } else { None };
    OsIdentity {
        process: (got_process && host.is_none()).then_some(process),
        user: if server {
            client_user(handle)
        } else {
            pipe_owner(handle)
        },
        group: None,
        session: got_session.then_some(session),
        host,
    }
}

/// The host a remote client came from; a local one has none.
fn client_host(server: HANDLE) -> Option<String> {
    let mut name = [0u16; 256];
    // SAFETY: `name` is writable for its size in bytes.
    let got = unsafe {
        GetNamedPipeClientComputerNameW(server, name.as_mut_ptr(), size_of_val(&name) as u32)
    };
    if got == 0 {
        debug_assert!(last_error() != 0 || ERROR_PIPE_LOCAL != 0);
        return None;
    }
    let length = name.iter().position(|unit| *unit == 0).unwrap_or(name.len());
    Some(String::from_utf16_lossy(&name[..length]))
}
