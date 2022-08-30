macro_rules! t {
    ($arg:expr) => (
        log::debug!("{}", $arg)
    );
    ($($arg:tt)+) => (
        log::debug!($($arg)+)
    );
}

pub(crate) use t;
