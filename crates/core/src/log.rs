#[macro_export]
macro_rules! log_info {
    ($ctx:expr, $msg:expr) => (
        log::info!(target: $ctx.id(), "[{:.3} INFO  {}] {}", $ctx.time(), $ctx.id(), $msg)
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::info!(target: $ctx.id(), concat!("[{:.3} INFO  {}] ", $format), $ctx.time(), $ctx.id(), $($arg)+)
    );
}

#[macro_export]
macro_rules! log_debug {
    ($ctx:expr, $msg:expr) => (
        log::debug!(target: $ctx.id(), "[{:.3} DEBUG {}] {}", $ctx.time(), $ctx.id(), $msg)
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::debug!(target: $ctx.id(), concat!("[{:.3} DEBUG {}] ", $format), $ctx.time(), $ctx.id(), $($arg)+)
    );
}

#[macro_export]
macro_rules! log_trace {
    ($ctx:expr, $msg:expr) => (
        log::trace!(target: $ctx.id(), "[{:.3} TRACE {}] {}", $ctx.time(), $ctx.id(), $msg)
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::trace!(target: $ctx.id(), concat!("[{:.3} TRACE {}] ", $format), $ctx.time(), $ctx.id(), $($arg)+)
    );
}

#[macro_export]
macro_rules! log_error {
    ($ctx:expr, $msg:expr) => (
        log::error!(target: $ctx.id(), "[{:.3} ERROR {}] {}", $ctx.time(), $ctx.id(), $msg)
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::error!(target: $ctx.id(), concat!("[{:.3} ERROR {}] ", $format), $ctx.time(), $ctx.id(), $($arg)+)
    );
}

#[macro_export]
macro_rules! log_warn {
    ($ctx:expr, $msg:expr) => (
        log::warn!(target: $ctx.id(), "[{:.3} WARN  {}] {}", $ctx.time(), $ctx.id(), $msg)
    );
    ($ctx:expr, $format:expr, $($arg:tt)+) => (
        log::warn!(target: $ctx.id(), concat!("[{:.3} WARN  {}] ", $format), $ctx.time(), $ctx.id(), $($arg)+)
    );
}
