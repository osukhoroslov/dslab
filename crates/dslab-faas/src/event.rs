//! Simulation events.
use serde::Serialize;

/// An idle container must be destroyed.
#[derive(Clone, Serialize)]
pub struct ContainerEndEvent {
    /// Container id.
    pub id: usize,
}

/// A deploying container is ready to start running.
#[derive(Clone, Serialize)]
pub struct ContainerStartEvent {
    /// Container id.
    pub id: usize,
}

/// A new prewarmed container is deployed.
#[derive(Clone, Serialize)]
pub struct IdleDeployEvent {
    /// Host id.
    pub id: usize,
    /// Expected number of invocations on host. If the real number is different, the container is not deployed.
    pub expected_invocation: u64,
}

/// A running invocation stops executing.
#[derive(Clone, Serialize)]
pub struct InvocationEndEvent {
    /// Invocation id.
    pub id: usize,
}

/// A new invocation starts executing.
#[derive(Clone, Serialize)]
pub struct InvocationStartEvent {
    /// Invocation id.
    pub id: usize,
    /// Function id.
    pub func_id: usize,
}

/// Simulation ends.
#[derive(Clone, Serialize)]
pub struct SimulationEndEvent {}
