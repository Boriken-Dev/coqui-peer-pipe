//! Deadlines from durations, where a duration too long for the clock means no deadline.
#![forbid(unsafe_code)]

use core::time::Duration;
use std::time::Instant;

/// `limit` from now; `None`, waiting forever, when the clock cannot hold it (`Duration::MAX` does
/// this). Computed before any operation starts, so nothing between its start and its end can panic.
pub(crate) fn after(limit: Duration) -> Option<Instant> {
    Instant::now().checked_add(limit)
}

/// [`after`] for an optional bound, `None` waiting forever as well.
pub(crate) fn within(limit: Option<Duration>) -> Option<Instant> {
    limit.and_then(after)
}

/// What is left until `deadline`, `None` for no deadline.
pub(crate) fn left(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|until| until.saturating_duration_since(Instant::now()))
}

/// Whether `deadline` has passed; never for no deadline.
pub(crate) fn passed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|until| Instant::now() >= until)
}
