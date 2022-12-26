//! Storage resource trait.
//!
//! This trait declares methods that every storage resource has, e.g. [`StorageResource::read()`] or
//! [`StorageResource::write()`], and some methods that return information about resource.

use dslab_core::Id;

/// Information about storage resource, including its capacity and current usage.
#[derive(Debug, PartialEq)]
pub struct StorageResourceInfo {
    /// Storage resource capacity. Is equal to `used_space` + `free_space`.
    pub capacity: u64,
    /// Amount of used space. Cannot be greater than `capacity`.
    pub used_space: u64,
    /// Amount of free space. Cannot be greater than `capacity`.
    pub free_space: u64,
}

/// Trait for storage resource.
pub trait StorageResource {
    /// Submits data read request and returns unique request id.
    ///
    /// The amount of data read from storage resource is specified in `size`.
    /// The component specified in `requester` will receive `DataReadCompleted` event upon the read completion. If the
    /// read size is larger than the storage resource capacity, `DataReadFailed` event will be immediately emitted instead. Note
    /// that the returned request id is unique only within the current storage resource.
    fn read(&mut self, size: u64, requester: Id) -> u64;

    /// Submits data write request and returns unique request id.
    ///
    /// The amount of data written to storage resource is specified in `size`.
    /// The component specified in `requester` will receive `DataWriteCompleted` event upon the write completion. If
    /// there is not enough available storage resource space, `DataWriteFailed` event will be immediately emitted instead.
    /// Note that the returned request id is unique only within the current storage resource.
    fn write(&mut self, size: u64, requester: Id) -> u64;

    /// Marks previously used storage resource space of given `size` as free.
    ///
    /// The `size` should not exceed the currently used storage resource space.
    fn mark_free(&mut self, size: u64) -> Result<(), String>;

    /// Returns the amount of used storage resource space.
    fn used_space(&self) -> u64;

    /// Returns the amount of free storage resource space.
    fn free_space(&self) -> u64;

    /// Returns the capacity of storage resource.
    fn capacity(&self) -> u64;

    /// Returns id of this storage resource.
    fn id(&self) -> Id;

    /// Returns struct with information about the storage resource.
    fn info(&self) -> StorageResourceInfo;
}
