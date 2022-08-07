//! Dataset reader interface.

// VM request to scheduler. Result will be converted into VirtualMachine structure.
#[derive(Clone)]
pub struct VMRequest {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub lifetime: f64,
    pub start_time: f64,
}

pub trait DatasetReader {
    /// Standard dataset reader interface to get next VM to schedule it.
    fn get_next_vm(&mut self) -> Option<VMRequest>;
}
