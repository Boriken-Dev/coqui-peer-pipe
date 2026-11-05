//! `PipeName` as a value: what is valid, its text form, its equality.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use coqui_peer_pipe::{Error, PipeName};

fn hash_of(name: &PipeName) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn a_name_is_send_and_sync() {
    assert_send_sync::<PipeName>();
}

#[test]
fn names_that_could_leave_their_namespace_are_refused() {
    for bad in [
        "", ".", "..", ".hidden", "a/b", r"a\b", "a b", "a:b", "ñ", "a\0b", "a\nb",
    ] {
        assert!(
            matches!(PipeName::named_in("/tmp", bad), Err(Error::InvalidName)),
            "{bad:?}"
        );
    }
}

#[test]
fn a_plain_name_is_valid_with_every_allowed_character() {
    let name = PipeName::named_in("/tmp", "Az09._-").unwrap();
    assert!(name.to_string().contains("09._-"));
}

#[test]
fn the_text_form_parses_back_to_an_equal_value() {
    let name = PipeName::named_in("/tmp", "round-trip").unwrap();
    let parsed: PipeName = name.to_string().parse().unwrap();
    assert_eq!(parsed, name);
    assert_eq!(hash_of(&parsed), hash_of(&name));
}

#[test]
fn text_that_names_nothing_is_refused() {
    for bad in ["", "plain", "http:x", "pipe:", "pipe:../x", "unix:"] {
        assert!(bad.parse::<PipeName>().is_err(), "{bad:?}");
    }
}

#[cfg(windows)]
mod windows {
    use super::*;

    #[test]
    fn a_name_maps_into_the_pipe_namespace() {
        let name = PipeName::named("service").unwrap();
        assert_eq!(name.path().to_str().unwrap(), r"\\.\pipe\service");
        assert_eq!(name.to_string(), "pipe:service");
    }

    #[test]
    fn a_directory_means_nothing_to_a_pipe() {
        assert_eq!(
            PipeName::named_in("/run/x", "service").unwrap(),
            PipeName::named("service").unwrap()
        );
    }

    #[test]
    fn two_cases_of_a_name_are_one_name() {
        let upper = PipeName::named("SeRvIcE").unwrap();
        let lower = PipeName::named("service").unwrap();
        assert_eq!(upper, lower);
        assert_eq!(hash_of(&upper), hash_of(&lower));
    }

    #[test]
    fn a_path_must_be_in_the_pipe_namespace() {
        for bad in [
            r"C:\x",
            r"x",
            r"\\.\x",
            r"\\.\pipe\",
            r"\\.\pipe\..",
            r"\\.\pipe\..\x",
            r"\\.\pipe\a\..\b",
            r"\\.\pipe\a/b",
            r"\\.\pipe\a\\b",
            r"\\\pipe\x",
            "\\\\.\\pipe\\a\nb",
        ] {
            assert!(
                matches!(PipeName::at(bad), Err(Error::InvalidName)),
                "{bad}"
            );
        }
        assert!(PipeName::at(r"\\.\pipe\group\name").is_ok());
        assert!(PipeName::at(r"\\HOST\PIPE\name").is_ok());
    }

    #[test]
    fn every_documented_spelling_parses() {
        let local = PipeName::named("service").unwrap();
        assert_eq!("pipe:service".parse::<PipeName>().unwrap(), local);
        assert_eq!(r"\\.\pipe\service".parse::<PipeName>().unwrap(), local);
        assert_eq!(
            r"pipe:group\name".parse::<PipeName>().unwrap().path(),
            std::path::Path::new(r"\\.\pipe\group\name")
        );
        assert!(matches!(
            "unix:/tmp/x".parse::<PipeName>(),
            Err(Error::Unsupported)
        ));
    }

    #[test]
    fn a_pipe_on_another_host_has_its_own_text_form() {
        let name = PipeName::on_host("host-1", "service").unwrap();
        assert_eq!(name.path().to_str().unwrap(), r"\\host-1\pipe\service");
        assert_eq!(name.to_string().parse::<PipeName>().unwrap(), name);
        assert!(matches!(
            PipeName::on_host(r"a\b", "x"),
            Err(Error::InvalidName)
        ));
        assert!(matches!(
            PipeName::on_host("", "x"),
            Err(Error::InvalidName)
        ));
    }
}

#[cfg(unix)]
mod unix {
    use std::process::Command;

    use super::*;

    #[test]
    fn a_name_in_a_directory_is_a_socket_path() {
        let name = PipeName::named_in("/run/app", "user").unwrap();
        assert_eq!(name.path().to_str().unwrap(), "/run/app/user.sock");
        assert_eq!(name.to_string(), "unix:/run/app/user.sock");
    }

    #[test]
    fn every_documented_spelling_parses() {
        let name = PipeName::at("/run/app/user.sock").unwrap();
        assert_eq!("unix:/run/app/user.sock".parse::<PipeName>().unwrap(), name);
        assert_eq!("/run/app/user.sock".parse::<PipeName>().unwrap(), name);
        assert!(matches!(
            r"\\.\pipe\x".parse::<PipeName>(),
            Err(Error::InvalidName)
        ));
    }

    #[test]
    fn a_path_too_long_for_a_socket_is_refused_when_named() {
        let long = format!("/tmp/{}", "a".repeat(200));
        assert!(matches!(PipeName::at(&long), Err(Error::InvalidName)));
    }

    #[test]
    fn a_path_with_no_file_name_is_refused() {
        assert!(matches!(PipeName::at("/"), Err(Error::InvalidName)));
        assert!(matches!(PipeName::at(""), Err(Error::InvalidName)));
    }

    #[test]
    fn another_host_is_not_a_unix_thing() {
        assert!(matches!(
            PipeName::on_host("h", "x"),
            Err(Error::Unsupported)
        ));
    }

    /// The test process's environment is left alone: a child reads the variables.
    fn named_in_child(variables: &[(&str, &str)]) -> String {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "unix::prints_a_name_when_asked", "--nocapture"])
            .env_remove("XDG_RUNTIME_DIR")
            .env_remove("TMPDIR")
            .env("COQUI_PEER_PIPE_TEST_PRINT_NAME", "1");
        for (name, value) in variables {
            command.env(name, value);
        }
        let output = command.output().unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("NAME=").map(str::to_owned))
            .unwrap()
    }

    #[test]
    fn prints_a_name_when_asked() {
        if std::env::var_os("COQUI_PEER_PIPE_TEST_PRINT_NAME").is_some() {
            match PipeName::named("x") {
                Ok(name) => println!("NAME={name}"),
                Err(Error::NoRuntimeDirectory) => println!("NAME=none"),
                Err(other) => panic!("{other}"),
            }
        }
    }

    #[test]
    fn named_takes_the_first_runtime_directory_set_to_an_absolute_path() {
        assert_eq!(
            named_in_child(&[("XDG_RUNTIME_DIR", "/run/user/7"), ("TMPDIR", "/tmp/t")]),
            "unix:/run/user/7/x.sock"
        );
        assert_eq!(
            named_in_child(&[("TMPDIR", "/tmp/t")]),
            "unix:/tmp/t/x.sock"
        );
        assert_eq!(
            named_in_child(&[("XDG_RUNTIME_DIR", "relative"), ("TMPDIR", "/tmp/t")]),
            "unix:/tmp/t/x.sock"
        );
        assert_eq!(named_in_child(&[]), "none");
    }
}
