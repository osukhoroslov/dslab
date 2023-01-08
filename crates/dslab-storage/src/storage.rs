//! Storage model.

use dslab_core::Id;

/// Information about storage, including its capacity and current usage.
#[derive(Debug, PartialEq)]
pub struct StorageInfo {
    /// Storage capacity. Is equal to `used_space` + `free_space`.
    pub capacity: u64,
    /// Amount of used space. Cannot be greater than `capacity`.
    pub used_space: u64,
    /// Amount of free space. Cannot be greater than `capacity`.
    pub free_space: u64,
}

/// A trait for modeling an abstract storage resource, i.e. any device, system or service for storing data.
///
/// The main operations are reading and writing data to/from storage. Implementations should model the delays associated with these operations.
/// The trait also includes operations to get information about the storage, including its capacity, current usage, etc.
pub trait Storage {
    /// Submits data read request and returns unique request id.
    ///
    /// The amount of data read from storage is specified in `size`.
    /// The component specified in `requester` will receive `DataReadCompleted` event upon the read completion.
    /// If the read size is larger than the storage capacity, `DataReadFailed` event will be immediately
    /// emitted instead.
    /// Note that the returned request id is unique only within the current storage.
    fn read(&mut self, size: u64, requester: Id) -> u64;

    /// Submits data write request and returns unique request id.
    ///
    /// The amount of data written to storage is specified in `size`.
    /// The component specified in `requester` will receive `DataWriteCompleted` event upon the write completion.
    /// If there is not enough available storage space, `DataWriteFailed` event will be immediately emitted
    /// instead.
    /// Note that the returned request id is unique only within the current storage.
    fn write(&mut self, size: u64, requester: Id) -> u64;

    /// Marks previously used storage space of given `size` as free.
    ///
    /// The `size` should not exceed the currently used storage space.
    fn mark_free(&mut self, size: u64) -> Result<(), String>;

    /// Returns the amount of used storage space.
    fn used_space(&self) -> u64;

    /// Returns the amount of free storage space.
    fn free_space(&self) -> u64;

    /// Returns the storage capacity.
    fn capacity(&self) -> u64;

    /// Returns identifier of simulation component representing the storage.
    fn id(&self) -> Id;

    /// Returns struct with information about the storage.
    fn info(&self) -> StorageInfo;
}
