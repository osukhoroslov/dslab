use serde::Serialize;

use crate::invocation::InvocationRequest;

#[derive(Serialize, Clone)]
pub struct ContainerEndEvent {
    pub id: u64,
    pub expected_count: u64,
}

#[derive(Serialize, Clone)]
pub struct ContainerStartEvent {
    pub id: u64,
}

#[derive(Serialize, Clone)]
pub struct IdleDeployEvent {
    pub id: u64,
}

#[derive(Serialize, Clone)]
pub struct InvocationEndEvent {
    pub id: u64,
}

#[derive(Serialize, Clone)]
pub struct InvocationStartEvent {
    pub request: InvocationRequest,
}

#[derive(Serialize, Clone)]
pub struct SimulationEndEvent {}
