//! Logging facilities.

use atty::Stream;
use colored::{Color, ColoredString, Colorize};
use log::error;
use serde_json::json;
use serde_type_name::type_name;

use crate::event::Event;

/// Applies the color to the string if stderr (log) goes to console.
pub fn get_colored(s: &str, color: Color) -> ColoredString {
    if atty::is(Stream::Stderr) {
        s.color(color)
    } else {
        s.normal()
    }
}

/// Logs a message at the info level.
///
/// # Examples
///
/// ```rust
/// use std::io::Write;
/// use env_logger::Builder;
/// use dslab_core::{log_info, Simulation, SimulationContext};///
///
/// struct Component {
///     ctx: SimulationContext,
/// }
///
/// impl Component {
///     fn start(&self) {
///         log_info!(self.ctx, "started");
///     }
/// }
///
/// // configure env_logger
/// Builder::from_default_env()
///     .format(|buf, record| writeln!(buf, "{}", record.args()))
///     .init();
///
/// let mut sim = Simulation::new(123);
/// let comp = Component { ctx: sim.create_context("comp") };
/// comp.start();
/// ```
#[macro_export]
macro_rules! log_info {
    ($ctx:expr, $msg:expr) => (
        log::info!(
            target: $ctx.name(),
            "[{:.3} {}  {}] {}",
            $ctx.time(), $crate::log::get_colored("INFO", $crate::colored::Color::Green), $ctx.name(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::info!(
            target: $ctx.name(),
            concat!("[{:.3} {}  {}] ", $format),
            $ctx.time(), $crate::log::get_colored("INFO", $crate::colored::Color::Green), $ctx.name(), $($arg)+
        )
    );
}

/// Logs a message at the debug level.
///
/// # Examples
///
/// See [`log_info!`](crate::log_info!).
#[macro_export]
macro_rules! log_debug {
    ($ctx:expr, $msg:expr) => (
        log::debug!(
            target: $ctx.name(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("DEBUG", $crate::colored::Color::Blue), $ctx.name(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::debug!(
            target: $ctx.name(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("DEBUG", $crate::colored::Color::Blue), $ctx.name(), $($arg)+
        )
    );
}

/// Logs a message at the trace level.
///
/// # Examples
///
/// See [`log_info!`](crate::log_info!).
#[macro_export]
macro_rules! log_trace {
    ($ctx:expr, $msg:expr) => (
        log::trace!(
            target: $ctx.name(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("TRACE", $crate::colored::Color::Cyan), $ctx.name(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::trace!(
            target: $ctx.name(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("TRACE", $crate::colored::Color::Cyan), $ctx.name(), $($arg)+
        )
    );
}

/// Logs a message at the error level.
///
/// # Examples
///
/// See [`log_info!`](crate::log_info!).
#[macro_export]
macro_rules! log_error {
    ($ctx:expr, $msg:expr) => (
        log::error!(
            target: $ctx.name(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("ERROR", $crate::colored::Color::Red), $ctx.name(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::error!(
            target: $ctx.name(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("ERROR", $crate::colored::Color::Red), $ctx.name(), $($arg)+
        )
    );
}

/// Logs a message at the warn level.
///
/// # Examples
///
/// See [`log_info!`](crate::log_info!).
#[macro_export]
macro_rules! log_warn {
    ($ctx:expr, $msg:expr) => (
        log::warn!(
            target: $ctx.name(),
            "[{:.3} {}  {}] {}",
            $ctx.time(), $crate::log::get_colored("WARN", $crate::colored::Color::Yellow), $ctx.name(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::warn!(
            target: $ctx.name(),
            concat!("[{:.3} {}  {}] ", $format),
            $ctx.time(), $crate::log::get_colored("WARN", $crate::colored::Color::Yellow), $ctx.name(), $($arg)+
        )
    );
}

/// Logs an unhandled event.
///
/// This method is used internally in [`cast!`](crate::cast!) macro.
pub fn log_unhandled_event(event: Event) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Unhandled event: {}",
        event.time,
        crate::log::get_colored("ERROR", colored::Color::Red),
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dst": event.dst})
    );
}

/// Logs an undelivered event.
pub(crate) fn log_undelivered_event(event: Event) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Undelivered event: {}",
        event.time,
        crate::log::get_colored("ERROR", colored::Color::Red),
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dst": event.dst})
    );
}

/// Logs incorrect event.
pub(crate) fn log_incorrect_event(event: Event, msg: &str) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Incorrect event ({}): {}",
        event.time,
        crate::log::get_colored("ERROR", colored::Color::Red),
        msg,
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dst": event.dst})
    );
}
