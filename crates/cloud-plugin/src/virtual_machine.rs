use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use log::info;

use crate::host::ReleaseVmResources;
use crate::network::MESSAGE_DELAY;
use crate::scheduler::VMFinished;

pub static VM_INIT_TIME: f64 = 1.0;
pub static VM_FINISH_TIME: f64 = 0.5;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VirtualMachine {
    pub id: String,
    pub cpu_usage: u32,
    pub ram_usage: u32,
    lifetime: f64,
    pub actor_id: ActorId,
}

impl VirtualMachine {
    pub fn new(id: &str, cpu: u32, ram: u32, lifetime: f64) -> Self {
        Self {
            id: id.to_string(),
            cpu_usage: cpu,
            ram_usage: ram,
            lifetime,
            actor_id: ActorId::from(&id),
        }
    }
}

// VM EVENTS ///////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VMInit {
    pub scheduler_id: ActorId,
}

#[derive(Debug)]
pub struct VMStart {
    pub host_actor_id: ActorId,
    pub scheduler_id: ActorId,
}

#[derive(Debug)]
pub struct VMAllocationFailed {
    pub reason: String,
}

#[derive(Debug)]
pub struct VMFinish {
    pub host_actor_id: ActorId,
    pub scheduler_id: ActorId,
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            VMInit { scheduler_id } => {
                ctx.emit_self(VMStart { host_actor_id: from, scheduler_id: scheduler_id.clone() },
                    VM_INIT_TIME
                );
            },
            VMStart { host_actor_id, scheduler_id } => {
                info!("[time = {}] vm #{} initialized and started", ctx.time(), self.id);
                ctx.emit_self(VMFinish { host_actor_id: host_actor_id.clone(),
                                         scheduler_id: scheduler_id.clone() }, self.lifetime
                );
            },
            VMAllocationFailed { reason } => {
                info!("[time = {}] vm #{} allocation failed due to: {}",
                          ctx.time(), self.id, reason);
            },
            VMFinish { host_actor_id, scheduler_id } => {
                info!("[time = {}] vm #{} stopped due to lifecycle end", ctx.time(), self.id);
                ctx.emit(VMFinished { host_id: host_actor_id.to_string(), vm: self.clone() },
                    scheduler_id.clone(),
                    MESSAGE_DELAY
                );
                ctx.emit(ReleaseVmResources { vm_id: self.id.clone() },
                    host_actor_id.clone(),
                    VM_FINISH_TIME
                );
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
