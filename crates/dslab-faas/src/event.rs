use serde::Serialize;

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
    pub id: usize,
    pub func_id: usize,
}

#[derive(Serialize)]
pub struct SimulationEndEvent {}
