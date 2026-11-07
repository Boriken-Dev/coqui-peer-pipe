//! A setting looser than its default is reported as data by `weakened()` and never warned about: the crate
//! reports and the caller decides.

mod support;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coqui_transport_pipe::{Listener, Stream};
use support::place;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

#[derive(Clone, Default)]
struct Recorder {
    warnings: Arc<Mutex<Vec<String>>>,
}

struct Setting(Option<String>);

impl Visit for Setting {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "setting" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

impl Subscriber for Recorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if *event.metadata().level() == Level::WARN {
            let mut setting = Setting(None);
            event.record(&mut setting);
            if let Some(name) = setting.0 {
                self.warnings.lock().unwrap().push(name);
            }
        }
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// The settings warned about while `body` runs.
fn warned(body: impl FnOnce()) -> Vec<String> {
    let recorder = Recorder::default();
    let warnings = recorder.warnings.clone();
    tracing::subscriber::with_default(recorder, body);
    warnings.lock().unwrap().clone()
}

#[test]
fn a_weakened_listener_is_reported_when_it_binds_and_not_warned_about() {
    let place = place("reported");
    let mut reported = Vec::new();
    let warnings = warned(|| {
        let options = Listener::options().read_timeout(None).write_timeout(None);
        reported = options.weakened();
        let listener = options.bind(&place.name).unwrap();
        assert_eq!(listener.weakened().len(), 2);
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    let settings: Vec<_> = reported.iter().map(|w| w.setting.to_string()).collect();
    assert_eq!(settings, ["read_timeout", "write_timeout"]);
    assert_eq!(reported[0].value, "none");
    assert_eq!(reported[0].strict, "30s");
}

#[test]
fn a_weakened_connection_is_reported_and_not_warned_about() {
    let place = place("reported-client");
    let _listener = Listener::bind(&place.name).unwrap();
    let mut reported = Vec::new();
    let warnings = warned(|| {
        let options = Stream::options().read_timeout(Some(Duration::from_secs(31)));
        reported = options.weakened();
        drop(options.connect(&place.name).unwrap());
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    let settings: Vec<_> = reported.iter().map(|w| w.setting.to_string()).collect();
    assert_eq!(settings, ["read_timeout"]);
}

#[test]
fn the_strict_defaults_report_nothing() {
    let place = place("reported-strict");
    let warnings = warned(|| {
        let listener = Listener::bind(&place.name).unwrap();
        assert!(listener.weakened().is_empty());
        let _client = Stream::connect(&place.name).unwrap();
        assert!(Stream::options().weakened().is_empty());
        drop(listener);
    });
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[cfg(windows)]
#[test]
fn allowing_remote_clients_is_reported_and_not_warned_about() {
    let place = place("reported-remote");
    let mut reported = Vec::new();
    let warnings = warned(|| {
        let options = Listener::options().allow_remote_clients(true);
        reported = options.weakened();
        drop(options.bind(&place.name).unwrap());
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].setting, "remote_clients");
}

#[cfg(windows)]
#[test]
fn a_socket_group_means_nothing_on_windows() {
    let place = place("group-windows");
    for options in [
        Listener::options().socket_group(1),
        Listener::options().socket_group_name("users"),
    ] {
        assert!(matches!(
            options.bind(&place.name),
            Err(coqui_transport_pipe::Error::InvalidOptions)
        ));
    }
}

/// A setting this platform cannot honour is never dropped: one that would be stricter than the default is
/// refused, one that is meaningless here is reported in `not_applied()` and not warned about.
#[cfg(windows)]
#[test]
fn a_unix_setting_is_refused_when_stricter_and_reported_when_meaningless() {
    use coqui_transport_pipe::Error;

    let place = place("not-applied");
    assert!(matches!(
        Listener::options().remove_stale(false).bind(&place.name),
        Err(Error::InvalidOptions)
    ));
    let mut reported = Vec::new();
    let warnings = warned(|| {
        let options = Listener::options().socket_mode(0o660).backlog(8);
        reported = options.not_applied();
        let listener = options.bind(&place.name).unwrap();
        assert_eq!(listener.not_applied().len(), 2);
        assert!(listener.weakened().is_empty());
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    let settings: Vec<_> = reported.iter().map(|w| w.setting.to_string()).collect();
    assert_eq!(settings, ["socket_mode", "backlog"]);
    assert_eq!(reported[0].value, "660");
    assert_eq!(reported[0].strict, "600");
}

#[cfg(unix)]
#[test]
fn a_windows_setting_is_refused_when_stricter_and_reported_when_meaningless() {
    use coqui_transport_pipe::Error;

    let place = place("not-applied");
    for options in [
        Listener::options().security_descriptor("D:P(A;;GA;;;WD)"),
        Listener::options().max_instances(2),
    ] {
        assert!(matches!(
            options.bind(&place.name),
            Err(Error::InvalidOptions)
        ));
    }
    let mut reported = Vec::new();
    let warnings = warned(|| {
        let options = Listener::options()
            .buffer_size(4096)
            .allow_remote_clients(true);
        reported = options.not_applied();
        let listener = options.bind(&place.name).unwrap();
        assert_eq!(listener.not_applied().len(), 2);
        assert!(listener.weakened().is_empty());
        let connect = Stream::options().allow_impersonation(true);
        assert_eq!(connect.not_applied().len(), 1);
        drop(connect.connect(&place.name).unwrap());
    });
    assert!(warnings.is_empty(), "{warnings:?}");
    let settings: Vec<_> = reported.iter().map(|w| w.setting.to_string()).collect();
    assert_eq!(settings, ["buffer_size", "remote_clients"]);
    assert_eq!(reported[0].value, "4096");
}

#[cfg(unix)]
#[test]
fn any_socket_mode_is_accepted_and_reported_when_it_is_not_the_default() {
    use std::os::unix::fs::MetadataExt;

    for mode in [0o400, 0o640, 0o660, 0o666, 0o700] {
        let place = place("mode");
        let mut reported = Vec::new();
        let warnings = warned(|| {
            let options = Listener::options().socket_mode(mode);
            reported = options.weakened();
            let listener = options.bind(&place.name).unwrap();
            let on_disk = std::fs::metadata(place.name.path()).unwrap().mode() & 0o7777;
            assert_eq!(on_disk, mode, "{mode:o}");
            drop(listener);
        });
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(reported.len(), 1, "{mode:o}");
        assert_eq!(reported[0].setting, "socket_mode");
        assert_eq!(reported[0].value, format!("{mode:o}"));
        assert_eq!(reported[0].strict, "600");
    }
    assert!(Listener::options().socket_mode(0o600).weakened().is_empty());
}
