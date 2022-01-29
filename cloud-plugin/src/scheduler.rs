use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use log::info;
use std::cell::RefCell;
use std::rc::Rc;

use crate::host::AllocationVerdict;
use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::placement_store::TryAllocateVM as TryAllocateVmOnStore;
use crate::store::Store;
use crate::virtual_machine::VirtualMachine;

use crate::network::MESSAGE_DELAY;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
    placement_store: ActorId,
    local_store: Store
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>,
               placement_store: ActorId) -> Self {
        let mut local_store = Store::new(monitoring.clone());
        for host in monitoring.borrow().get_hosts_list() {
            local_store.add_host(host.to_string(),
                &monitoring.borrow().get_host_state(ActorId::from(host)));
        }

        Self {
            id,
            monitoring: monitoring.clone(),
            placement_store,
            local_store: Store::new(monitoring.clone())
        }
    }

    pub fn add_host(&mut self, id: String, state: HostState) {
        self.local_store.add_host(id.clone(), &state);
    }

    fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.local_store.place_vm(&vm, &host_id);
    }

    fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.local_store.remove_vm(&vm, &host_id);
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

#[derive(Debug)]
pub struct ReplicateNewHost {
    pub id: String,
    pub host: HostState
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.local_store.clone().get_hosts_list() {   
                    if self.local_store.can_allocate(&vm, &host) == AllocationVerdict::Success {
                        info!(
                            "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(),
                            self.id,
                            vm.id,
                            host
                        );
                        found = true;
                        self.local_store.place_vm(&vm, &host);

                        ctx.emit(TryAllocateVmOnStore { vm: vm.clone(),
                                                          host_id: host.to_string() },
                                 self.placement_store.clone(), MESSAGE_DELAY);
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
            ReplicateNewHost {id, host } => {
                info!("[time = {}] new host #{} added to scheduler #{}", ctx.time(), id, self.id);
                self.add_host(id.clone(), host.clone());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
