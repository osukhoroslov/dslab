#[derive(PartialEq)]
pub enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughMemory,
    Success,
    HostNotFound,
}
