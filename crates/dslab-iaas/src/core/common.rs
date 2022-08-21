//! Common data structures.

use serde::Serialize;

/// Describes a specific resource allocation, is used to pass and store VM resource requirements.
#[derive(Serialize, Clone)]
pub struct Allocation {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
}

/// Describes a result of checking the allocation feasibility.
#[derive(PartialEq)]
pub enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughMemory,
    Success,
    HostNotFound,
}
