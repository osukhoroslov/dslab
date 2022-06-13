#[derive(Clone)]
pub struct VMRequest {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub lifetime: f64,
    pub start_time: f64,
}

pub trait DatasetReader {
    fn get_next_vm(&mut self) -> Option<VMRequest>;
}
