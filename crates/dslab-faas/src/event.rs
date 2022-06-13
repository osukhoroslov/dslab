use serde::Serialize;

use crate::invocation::InvocationRequest;

#[derive(Serialize)]
pub struct ContainerEndEvent {
    pub id: u64,
    pub expected_count: u64,
}

#[derive(Serialize)]
pub struct ContainerStartEvent {
    pub id: u64,
}

#[derive(Serialize)]
pub struct IdleDeployEvent {
    pub id: u64,
}

#[derive(Serialize)]
pub struct InvocationEndEvent {
    pub id: u64,
}

#[derive(Serialize)]
pub struct InvocationStartEvent {
    pub request: InvocationRequest,
}

#[derive(Serialize)]
pub struct SimulationEndEvent {}
