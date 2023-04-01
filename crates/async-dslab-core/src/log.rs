use dslab_core::Event;
use log::error;
use serde_json::json;
use serde_type_name::type_name;

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
