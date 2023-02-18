use serde::Serialize;

use crate::invocation::InvocationRequest;

#[derive(Serialize)]
pub struct ContainerEndEvent {
    pub id: usize,
    pub expected_count: usize,
}

#[derive(Serialize)]
pub struct ContainerStartEvent {
    pub id: usize,
}

#[derive(Serialize)]
pub struct IdleDeployEvent {
    pub id: usize,
}

#[derive(Serialize)]
pub struct InvocationEndEvent {
    pub id: usize,
}

#[derive(Serialize)]
pub struct InvocationStartEvent {
    pub request: InvocationRequest,
}

#[derive(Serialize)]
pub struct SimulationEndEvent {}
