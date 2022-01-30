use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use log::info;

use crate::host::ReleaseVmResources;
use crate::host::VMFinished;
use crate::network::MESSAGE_DELAY;

pub static VM_INIT_TIME: f64 = 1.0;
pub static VM_FINISH_TIME: f64 = 0.5;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VirtualMachine {
    pub id: String,
    pub cpu_usage: u64,
    pub memory_usage: u64,
    lifetime: f64,
    pub actor_id: ActorId,
}

impl VirtualMachine {
    pub fn new(id: &str, cpu: u64, memory: u64, lifetime: f64) -> Self {
        Self {
            id: id.to_string(),
            cpu_usage: cpu,
            memory_usage: memory,
            lifetime,
            actor_id: ActorId::from(&id),
        }
    }
}

// VM EVENTS ///////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VMInit {}

#[derive(Debug)]
pub struct VMStart {
    pub host_actor_id: ActorId,
}

#[derive(Debug)]
pub struct VMAllocationFailed {
    pub reason: String,
}

#[derive(Debug)]
pub struct VMFinish {
    pub host_actor_id: ActorId,
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            VMInit { } => {
                ctx.emit_self(VMStart { host_actor_id: from },
                    VM_INIT_TIME
                );
            },
            VMStart { host_actor_id } => {
                info!("[time = {}] vm #{} initialized and started", ctx.time(), self.id);
                ctx.emit_self(VMFinish { host_actor_id: host_actor_id.clone() }, self.lifetime);
            },
            VMAllocationFailed { reason } => {
                info!("[time = {}] vm #{} allocation failed due to: {}",
                          ctx.time(), self.id, reason);
            },
            VMFinish { host_actor_id } => {
                info!("[time = {}] vm #{} stopped due to lifecycle end", ctx.time(), self.id);
                ctx.emit(ReleaseVmResources { vm_id: self.id.clone() },
                    host_actor_id.clone(),
                    VM_FINISH_TIME
                );
                ctx.emit(VMFinished { vm: self.clone() }, host_actor_id.clone(), MESSAGE_DELAY);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
