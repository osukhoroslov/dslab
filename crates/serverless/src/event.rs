use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::invocation::InvocationRequest;

pub struct ContainerEndEvent {
    pub id: u64,
    pub expected_count: u64,
}

impl Serialize for ContainerEndEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ContainerEndEvent", 2)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("expected_count", &self.expected_count)?;
        state.end()
    }
}

pub struct ContainerStartEvent {
    pub id: u64,
}

impl Serialize for ContainerStartEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ContainerStartEvent", 1)?;
        state.serialize_field("id", &self.id)?;
        state.end()
    }
}

pub struct IdleDeployEvent {
    pub id: u64,
}

impl Serialize for IdleDeployEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IdleDeployEvent", 1)?;
        state.serialize_field("id", &self.id)?;
        state.end()
    }
}

pub struct InvocationEndEvent {
    pub id: u64,
}

impl Serialize for InvocationEndEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("InvocationEndEvent", 1)?;
        state.serialize_field("id", &self.id)?;
        state.end()
    }
}

pub struct InvocationStartEvent {
    pub request: InvocationRequest,
}

impl Serialize for InvocationStartEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("InvocationStartEvent", 1)?;
        state.serialize_field("request", &self.request)?;
        state.end()
    }
}

pub struct SimulationEndEvent {}

impl Serialize for SimulationEndEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let state = serializer.serialize_struct("SimulationEndEvent", 0)?;
        state.end()
    }
}
