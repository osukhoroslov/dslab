use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted};
use dslab_core::{async_core::shared_state::DetailsKey, event::EventData};

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

pub fn get_compute_start_id(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<CompStarted>().unwrap();
    event.id
}

pub fn get_compute_finished_id(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<CompFinished>().unwrap();
    event.id
}

pub fn get_compute_failed_id(data: &dyn EventData) -> DetailsKey {
    let event = data.downcast_ref::<CompFailed>().unwrap();
    event.id
}
