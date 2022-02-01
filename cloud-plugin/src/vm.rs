use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::events::{AllocationReleaseRequest, VMDeleteRequest, VMDeleted, VMStartRequest, VMStarted};

pub static VM_START_TIME: f64 = 1.0;
pub static VM_DELETE_TIME: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct VirtualMachine {
    pub id: String,
    pub actor_id: ActorId,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    lifetime: f64,
    host: Option<ActorId>,
}

impl VirtualMachine {
    pub fn new(id: &str, cpu: u32, memory: u64, lifetime: f64) -> Self {
        Self {
            id: id.to_string(),
            actor_id: ActorId::from(&id),
            cpu_usage: cpu,
            memory_usage: memory,
            lifetime,
            host: None,
        }
    }
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            VMStartRequest { host_id } => {
                self.host = Some(ActorId::from(host_id));
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
