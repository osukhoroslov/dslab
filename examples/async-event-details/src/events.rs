use serde::Serialize;

use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted};
use dslab_core::async_core::await_details::EventKey;

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

pub fn get_compute_start_id(event: &CompStarted) -> EventKey {
    event.id
}

pub fn get_compute_finished_id(event: &CompFinished) -> EventKey {
    event.id
}

pub fn get_compute_failed_id(event: &CompFailed) -> EventKey {
    event.id
}
