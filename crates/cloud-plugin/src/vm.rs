use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::events::allocation::AllocationReleaseRequest;
use crate::events::vm::{VMDeleteRequest, VMDeleted, VMStartRequest, VMStarted};

use crate::load_model::LoadModel;

pub static VM_START_TIME: f64 = 1.0;
pub static VM_DELETE_TIME: f64 = 0.5;

#[derive(Clone, Debug)]
pub struct VirtualMachine {
    pub id: String,
    pub actor_id: ActorId,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    start_timestamp: f64,
    lifetime: f64,
    host: Option<ActorId>,
    cpu_load_model: Box<dyn LoadModel>,
    memory_load_model: Box<dyn LoadModel>,
}

impl VirtualMachine {
    pub fn new(
        id: &str,
        cpu: u32,
        memory: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
    ) -> Self {
        Self {
            id: id.to_string(),
            actor_id: ActorId::from(&id),
            cpu_usage: cpu,
            memory_usage: memory,
            start_timestamp: 0.,
            lifetime,
            host: None,
            cpu_load_model,
            memory_load_model,
        }
    }

    fn set_start_timestamp(&mut self, timestamp: f64) {
        self.start_timestamp = timestamp;
    }

    pub fn get_current_cpu_load(&self, timestamp: f64) -> f64 {
        return self
            .cpu_load_model
            .get_resource_load(timestamp, timestamp - self.start_timestamp);
    }

    pub fn get_current_memory_load(&self, timestamp: f64) -> f64 {
        return self
            .memory_load_model
            .get_resource_load(timestamp, timestamp - self.start_timestamp);
    }
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            VMStartRequest { host_id } => {
                self.host = Some(ActorId::from(host_id));
                self.set_start_timestamp(ctx.time());

                // emit started event after startup delay
                ctx.emit(
                    VMStarted { vm_id: self.id.clone() },
                    self.host.clone().unwrap(),
                    VM_START_TIME,
                );
                // schedule release event after specified lifetime
                ctx.emit(
                    AllocationReleaseRequest { vm: self.clone() },
                    self.host.clone().unwrap(),
                    VM_START_TIME + self.lifetime,
                );
            }
            VMDeleteRequest {} => {
                ctx.emit(
                    VMDeleted { vm_id: self.id.clone() },
                    self.host.clone().unwrap(),
                    VM_DELETE_TIME,
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
