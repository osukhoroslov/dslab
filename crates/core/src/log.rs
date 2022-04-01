use atty::Stream;
use colored::{Color, ColoredString, Colorize};

pub fn get_colored(s: &str, color: Color) -> ColoredString {
    if atty::is(Stream::Stderr) {
        s.color(color)
    } else {
        s.normal()
    }
}

#[macro_export]
macro_rules! log_info {
    ($ctx:expr, $msg:expr) => (
        log::info!(
            target: $ctx.id(),
            "[{:.3} {}  {}] {}",
            $ctx.time(), $crate::log::get_colored("INFO", $crate::colored::Color::Green), $ctx.id(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::info!(
            target: $ctx.id(),
            concat!("[{:.3} {}  {}] ", $format),
            $ctx.time(), $crate::log::get_colored("INFO", $crate::colored::Color::Green), $ctx.id(), $($arg)+
        )
    );
}

#[macro_export]
macro_rules! log_debug {
    ($ctx:expr, $msg:expr) => (
        log::debug!(
            target: $ctx.id(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("DEBUG", $crate::colored::Color::Blue), $ctx.id(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::debug!(
            target: $ctx.id(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("DEBUG", $crate::colored::Color::Blue), $ctx.id(), $($arg)+
        )
    );
}

#[macro_export]
macro_rules! log_trace {
    ($ctx:expr, $msg:expr) => (
        log::trace!(
            target: $ctx.id(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("TRACE", $crate::colored::Color::Cyan), $ctx.id(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::trace!(
            target: $ctx.id(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("TRACE", $crate::colored::Color::Cyan), $ctx.id(), $($arg)+
        )
    );
}

#[macro_export]
macro_rules! log_error {
    ($ctx:expr, $msg:expr) => (
        log::error!(
            target: $ctx.id(),
            "[{:.3} {} {}] {}",
            $ctx.time(), $crate::log::get_colored("ERROR", $crate::colored::Color::Red), $ctx.id(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::error!(
            target: $ctx.id(),
            concat!("[{:.3} {} {}] ", $format),
            $ctx.time(), $crate::log::get_colored("ERROR", $crate::colored::Color::Red), $ctx.id(), $($arg)+
        )
    );
}

#[macro_export]
macro_rules! log_warn {
    ($ctx:expr, $msg:expr) => (
        log::warn!(
            target: $ctx.id(),
            "[{:.3} {}  {}] {}",
            $ctx.time(), $crate::log::get_colored("WARN", $crate::colored::Color::Yellow), $ctx.id(), $msg
        )
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::warn!(
            target: $ctx.id(),
            concat!("[{:.3} {}  {}] ", $format),
            $ctx.time(), $crate::log::get_colored("WARN", $crate::colored::Color::Yellow), $ctx.id(), $($arg)+
        )
    );
}
