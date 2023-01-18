use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct ContainerEndEvent {
    pub id: usize,
    pub expected_count: usize,
}

#[derive(Serialize, Clone)]
pub struct ContainerStartEvent {
    pub id: usize,
}

#[derive(Serialize, Clone)]
pub struct IdleDeployEvent {
    pub id: usize,
}

#[derive(Serialize, Clone)]
pub struct InvocationEndEvent {
    pub id: usize,
}

#[derive(Serialize, Clone)]
pub struct InvocationStartEvent {
    pub id: usize,
    pub func_id: usize,
}

#[derive(Serialize, Clone)]
pub struct SimulationEndEvent {}
