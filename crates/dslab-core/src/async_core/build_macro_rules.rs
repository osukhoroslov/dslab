/// wrap code that will be built for async dslab-core functionality
#[macro_export]
macro_rules! async_core {
    ($($item:item)*) => {
        $(#[cfg(feature = "async_core")]
        $item)*
    }
}

/// wrap code that will be built only if async functionality is disabled
#[macro_export]
macro_rules! async_disabled {
    ($($item:item)*) => {
        $(#[cfg(not(feature = "async_core"))]
        $item)*
    }
}
