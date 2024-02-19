use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Start {}

#[derive(Serialize, Clone)]
pub struct TaskCompleted {}

#[derive(Serialize, Clone)]
pub struct TaskRequest {
    pub flops: f64,
    pub memory: u64,
    pub cores: u32,
}
