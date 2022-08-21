//! Trait for dataset readers.

/// Represents information about a single virtual machine from dataset.
#[derive(Clone)]
pub struct VMRequest {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub lifetime: f64,
    pub start_time: f64,
}

pub trait DatasetReader {
    /// Returns the next VM from dataset (if any).
    ///
    /// VMs should be returned in non-decreasing order of their start times.
    fn get_next_vm(&mut self) -> Option<VMRequest>;
}
