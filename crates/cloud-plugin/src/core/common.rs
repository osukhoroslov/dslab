use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Allocation {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
}

#[derive(PartialEq)]
pub enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughMemory,
    Success,
    HostNotFound,
}
