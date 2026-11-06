//! A setting looser than its default is logged when it takes effect, and the strict defaults log nothing.

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

fn logged(body: impl FnOnce()) -> Vec<String> {
    let recorder = Recorder::default();
    let warnings = recorder.warnings.clone();
    tracing::subscriber::with_default(recorder, body);
    warnings.lock().unwrap().clone()
}

#[test]
fn a_weakened_listener_is_logged_when_it_binds() {
    let place = place("logged");
    let settings = logged(|| {
        let options = Listener::options().read_timeout(None).write_timeout(None);
        drop(options.bind(&place.name).unwrap());
    });
    assert_eq!(settings, ["read_timeout", "write_timeout"]);
}

#[test]
fn a_weakened_connection_is_logged_when_it_connects() {
    let place = place("logged-client");
    let _listener = Listener::bind(&place.name).unwrap();
    let settings = logged(|| {
        let options = Stream::options().read_timeout(Some(Duration::from_secs(31)));
        drop(options.connect(&place.name).unwrap());
    });
    assert_eq!(settings, ["read_timeout"]);
}

#[test]
fn the_strict_defaults_log_nothing() {
    let place = place("logged-strict");
    let settings = logged(|| {
        let listener = Listener::bind(&place.name).unwrap();
        let _client = Stream::connect(&place.name).unwrap();
        drop(listener);
    });
    assert!(settings.is_empty(), "{settings:?}");
}

#[cfg(windows)]
#[test]
fn allowing_remote_clients_is_logged() {
    let place = place("logged-remote");
    let settings = logged(|| {
        drop(
            Listener::options()
                .allow_remote_clients(true)
                .bind(&place.name)
                .unwrap(),
        );
    });
    assert_eq!(settings, ["remote_clients"]);
}
