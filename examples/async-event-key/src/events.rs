use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct Start {}

#[derive(Serialize, Clone)]
pub struct TaskRequest {
    pub cores: u32,
    pub memory: u64,
    pub flops: f64,
}

#[derive(Serialize, Clone)]
pub struct TaskCompleted {}
