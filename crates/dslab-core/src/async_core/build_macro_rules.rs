/// Macro to wrap a code that will be built for async-core feature.
#[macro_export]
macro_rules! async_enabled {
    ($($item:item)*) => {
        $(#[cfg(feature = "async_core")]
        $item)*
    }
}

/// Macro to wrap a code that will be built only if async-core feature is disabled.
#[macro_export]
macro_rules! async_disabled {
    ($($item:item)*) => {
        $(#[cfg(not(feature = "async_core"))]
        $item)*
    }
}
