/// wrap code that will be built for async dslab-core functionality
#[macro_export]
macro_rules! async_core {
    ($($item:item)*) => {
        $(#[cfg(feature = "async_core")]
        $item)*
    }
}

/// wrap code that will be built for async dslab-core functionality, but not for
/// async-details functionality
#[macro_export]
macro_rules! async_only_core {
    ($($item:item)*) => {
        $(#[cfg(all(feature = "async_core", not(feature = "async_details_core")))]
        $item)*
    }
}

/// wrap code that will be build for async-details dslab-core functionality
#[macro_export]
macro_rules! async_details_core {
    ($($item:item)*) => {
        $(#[cfg(feature = "async_details_core")]
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
