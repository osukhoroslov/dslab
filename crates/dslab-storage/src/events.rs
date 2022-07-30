//! Events sent by disk and file system.

use serde::Serialize;

// Disk events

#[derive(Serialize)]
/// Event type for disk read request complete. It is sent from disk to requester.
pub struct DataReadCompleted {
    /// Id which was returned from `read` disk method.
    pub request_id: u64,
    /// Size which was read from disk.
    pub size: u64,
}

#[derive(Serialize)]
/// Event type of disk read request failure. It is sent from disk to requester.
pub struct DataReadFailed {
    /// Id which was returned from `read` disk method.
    pub request_id: u64,
    /// Reason of failure.
    pub error: String,
}

#[derive(Serialize)]
/// Event type of disk write request complete. It is sent from disk to requester.
pub struct DataWriteCompleted {
    /// Id which was returned from `write` disk method.
    pub request_id: u64,
    /// Size which was written to disk.
    pub size: u64,
}

#[derive(Serialize)]
/// Event type of disk write request failure. It is sent from disk to requester.
pub struct DataWriteFailed {
    /// Id which was returned from `write` disk method.
    pub request_id: u64,
    /// Reason of failure.
    pub error: String,
}

// File events

#[derive(Serialize)]
/// Event type for file system read request complete. It is sent from file system to requester.
pub struct FileReadCompleted {
    /// Id which was returned from `read` file system method.
    pub request_id: u64,
    /// Name of the file to read from.
    pub file_name: String,
    /// Size which was read from disk.
    pub read_size: u64,
}

#[derive(Serialize)]
/// Event type for file system read request failure. It is sent from file system to requester.
pub struct FileReadFailed {
    /// Id which was returned from `read` file system method.
    pub request_id: u64,
    /// Name of the file to read from.
    pub file_name: String,
    /// Reason of failure.
    pub error: String,
}

#[derive(Serialize)]
/// Event type for file system write request complete. It is sent from file system to requester.
pub struct FileWriteCompleted {
    /// Id which was returned from `write` file system method.
    pub request_id: u64,
    /// Name of the file to write to.
    pub file_name: String,
    /// Size which was written to disk.
    pub new_size: u64,
}

#[derive(Serialize)]
/// Event type for file system write request failure. It is sent from file system to requester.
pub struct FileWriteFailed {
    /// Id which was returned from `write` file system method.
    pub request_id: u64,
    /// Name of the file to write to.
    pub file_name: String,
    /// Reason of failure.
    pub error: String,
}
