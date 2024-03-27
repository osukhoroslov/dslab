/// Macro to wrap a code that will be built if async mode is enabled.
#[macro_export]
macro_rules! async_mode_enabled {
    ($($item:item)*) => {
        $(#[cfg(feature = "async_mode")]
        $item)*
    }
}

/// Macro to wrap a code that will be built if async mode is disabled.
#[macro_export]
macro_rules! async_mode_disabled {
    ($($item:item)*) => {
        $(#[cfg(not(feature = "async_mode"))]
        $item)*
    }
}
