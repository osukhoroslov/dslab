//! Common structures.

use serde::Serialize;

/// VM capacity, is passed into processing events to briefly describe VM
#[derive(Serialize, Clone)]
pub struct Allocation {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
}

/// Allocation verdict.
#[derive(PartialEq)]
pub enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughMemory,
    Success,
    HostNotFound,
}
