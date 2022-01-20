use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use log::info;
use std::cell::RefCell;
use std::rc::Rc;

use crate::host::AllocationVerdict;
use crate::monitoring::Monitoring;
use crate::placement_storage::TryAllocateVM as TryAllocateVmOnStorage;
use crate::storage::Storage;
use crate::virtual_machine::VirtualMachine;

use crate::network::MESSAGE_DELAY;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
    placement_storage: ActorId,
    storage: Storage
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>,
               placement_storage: ActorId) -> Self {
        Self {
            id,
            monitoring: monitoring.clone(),
            placement_storage,
            storage: Storage::new(monitoring.clone())
        }
    }

    pub fn add_host(&mut self, id: String, cpu_full: u32, ram_full: u32) {
        self.storage.add_host(id.clone(), cpu_full, ram_full);
    }

    fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.storage.place_vm(&vm, &host_id);
    }

    fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.storage.remove_vm(&vm, &host_id);
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine,
}

#[derive(Debug, Clone)]
pub struct VMAllocationSucceeded {
    pub vm: VirtualMachine,
    pub host_id: String,
}

#[derive(Debug, Clone)]
pub struct VMAllocationFailed {
    pub vm: VirtualMachine,
    pub host_id: String,
}

#[derive(Debug, Clone)]
pub struct VMFinished {
    pub vm: VirtualMachine,
    pub host_id: String,
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.monitoring.borrow().get_hosts_list() {   
                    if self.storage.can_allocate(&vm, &host) == AllocationVerdict::Success {
                        info!(
                            "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(),
                            self.id,
                            vm.id,
                            host
                        );
                        found = true;
                        self.storage.place_vm(&vm, &host);

                        ctx.emit(TryAllocateVmOnStorage { vm: vm.clone(),
                                                          host_id: host.to_string() },
                                 self.placement_storage.clone(), MESSAGE_DELAY);
                        break;
                    }
                }
                if !found {
                    info!(
                        "[time = {}] scheduler #{} failed to pack vm #{}",
                        ctx.time(),
                        self.id,
                        vm.id
                    );

                    ctx.emit_self(FindHostToAllocateVM { vm: vm.clone() }, ALLOCATION_RETRY_PERIOD);
                }
            }
            VMAllocationSucceeded { vm, host_id } => {
                self.place_vm(&vm, &host_id);
            }
            VMAllocationFailed { vm, host_id } => {
                self.remove_vm(&vm, &host_id);
                ctx.emit_now(FindHostToAllocateVM { vm: vm.clone() }, ctx.id.clone());
            }
            VMFinished { vm, host_id } => {
                self.remove_vm(&vm, &host_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
