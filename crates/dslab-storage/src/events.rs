//! Events produced by storage resource and file system.

use serde::Serialize;

// Storage resource events

#[derive(Serialize)]
/// Corresponds to completion of storage resource read request. Source: storage resource, destination: requester.
pub struct DataReadCompleted {
    /// Request id returned by [`crate::resource::StorageResource::read()`] method.
    pub request_id: u64,
    /// Size of data read from storage resource.
    pub size: u64,
}

#[derive(Serialize)]
/// Corresponds to failure of storage resource read request. Source: storage resource, destination: requester.
pub struct DataReadFailed {
    /// Request id returned by [`crate::resource::StorageResource::read()`] method.
    pub request_id: u64,
    /// Reason of failure.
    pub error: String,
}

#[derive(Serialize)]
/// Corresponds to completion of storage resource write request. Source: storage resource, destination: requester.
pub struct DataWriteCompleted {
    /// Request id returned by [`crate::resource::StorageResource::write()`] method.
    pub request_id: u64,
    /// Size of data written to storage resource.
    pub size: u64,
}

#[derive(Serialize)]
/// Corresponds to failure of storage resource write request. Source: storage resource, destination: requester.
pub struct DataWriteFailed {
    /// Request id returned by [`crate::resource::StorageResource::write()`] method.
    pub request_id: u64,
    /// Reason of failure.
    pub error: String,
}

// File events

#[derive(Serialize)]
/// Corresponds to completion of file system read request. Source: file system, destination: requester.
pub struct FileReadCompleted {
    /// Request id returned by [`crate::fs::FileSystem::read()`] method.
    pub request_id: u64,
    /// Path to read file.
    pub file_path: String,
    /// Size of read data.
    pub read_size: u64,
}

#[derive(Serialize)]
/// Corresponds to failure of file system read request. Source: file system, destination: requester.
pub struct FileReadFailed {
    /// Id which was returned from `read` file system method.
    pub request_id: u64,
    /// Path to read file.
    pub file_path: String,
    /// Reason of failure.
    pub error: String,
}

#[derive(Serialize)]
/// Corresponds to completion of file system write request. Source: file system, destination: requester.
pub struct FileWriteCompleted {
    /// Id which was returned from `write` file system method.
    pub request_id: u64,
    /// Path to written file.
    pub file_path: String,
    /// Size of written data.
    pub new_size: u64,
}

#[derive(Serialize)]
/// Corresponds to failure of file system write request. Source: file system, destination: requester.
pub struct FileWriteFailed {
    /// Id which was returned from `write` file system method.
    pub request_id: u64,
    /// Path to written file.
    pub file_path: String,
    /// Reason of failure.
    pub error: String,
}
