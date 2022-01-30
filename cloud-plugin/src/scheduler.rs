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

    #[allow(dead_code)]
    monitoring: Rc<RefCell<Monitoring>>,

    placement_store: ActorId,
    local_store: Store,
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>, placement_store: ActorId) -> Self {
        Self {
            id,
            monitoring: monitoring.clone(),
            placement_store,
            local_store: Store::new(),
        }
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
    pub host: HostState,
}

#[derive(Debug)]
pub struct ReceiveSnapshot {
    pub local_store: Store,
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.local_store.get_hosts_list() {
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

                        ctx.emit(
                            TryAllocateVmOnStore {
                                vm: vm.clone(),
                                host_id: host.to_string(),
                            },
                            self.placement_store.clone(),
                            MESSAGE_DELAY,
                        );
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
                self.local_store.place_vm(&vm, &host_id);
            }
            VMAllocationFailed { vm, host_id } => {
                self.local_store.remove_vm(&vm, &host_id);
                ctx.emit_now(FindHostToAllocateVM { vm: vm.clone() }, ctx.id.clone());
            }
            VMFinished { vm, host_id } => {
                self.local_store.remove_vm(&vm, &host_id);
            }
            ReplicateNewHost { id, host } => {
                info!(
                    "[time = {}] new host #{} added to scheduler #{}",
                    ctx.time(),
                    id,
                    self.id
                );
                self.local_store.add_host(id.clone(), &host);
            }
            ReceiveSnapshot { local_store } => {
                self.local_store = local_store.clone();
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
