use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Start {}

#[derive(Serialize, Clone)]
pub struct TaskRequest {
    pub id: u64,
    pub cores: u32,
    pub memory: u64,
    pub flops: f64,
}
