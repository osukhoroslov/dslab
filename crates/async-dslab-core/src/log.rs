use dslab_core::Event;
use log::error;
use serde_json::json;
use serde_type_name::type_name;

/// Logs an undelivered event.
pub(crate) fn log_undelivered_event(event: Event) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Undelivered event: {}",
        event.time,
        dslab_core::log::get_colored("ERROR", colored::Color::Red),
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dest": event.dest})
    );
}

/// Logs incorrect event.
pub(crate) fn log_incorrect_event(event: Event, msg: &str) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Incorrect event ({}): {}",
        event.time,
        dslab_core::log::get_colored("ERROR", colored::Color::Red),
        msg,
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dest": event.dest})
    );
}
