use log::info;

use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::host::AllocationVerdict;
use crate::host::TryAllocateVM as TryAllocateVMOnHost;
use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::network::MESSAGE_DELAY;
use crate::scheduler::ReceiveSnapshot;
use crate::scheduler::ReplicateNewHost;
use crate::scheduler::VMAllocationFailed as ReportAllocationFailure;
use crate::scheduler::VMAllocationSucceeded;
use crate::scheduler::VMFinished as DropVMOnScheduler;
use crate::store::Store;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct PlacementStore {
    global_store: Store,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl PlacementStore {
    pub fn new(monitoring: Rc<RefCell<Monitoring>>) -> Self {
        let mut global_store = Store::new();
        for host in monitoring.borrow().get_hosts_list() {
            global_store.add_host(
                host.to_string(),
                &monitoring.borrow().get_host_state(ActorId::from(host)),
            );
        }

        Self {
            global_store,
            monitoring: monitoring.clone(),
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TryAllocateVM {
    pub vm: VirtualMachine,
    pub host_id: String,
}

#[derive(Debug)]
pub struct VMAllocationFailed {
    pub vm: VirtualMachine,
    pub host_id: String,
}

#[derive(Debug)]
pub struct VMFinished {
    pub vm: VirtualMachine,
    pub host_id: String,
}

#[derive(Debug)]
pub struct OnNewHostAdded {
    pub id: String,
    pub host: HostState,
}

#[derive(Debug)]
pub struct OnNewSchedulerAdded {
    pub scheduler_id: ActorId,
}

impl Actor for PlacementStore {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            TryAllocateVM { vm, host_id } => {
                if self.global_store.can_allocate(vm, host_id) == AllocationVerdict::Success {
                    self.global_store.place_vm(vm, host_id);
                    info!(
                        "[time = {}] vm #{} commited to host #{} in placement store",
                        ctx.time(),
                        vm.id,
                        host_id
                    );
                    ctx.emit(
                        TryAllocateVMOnHost {
                            vm: vm.clone(),
                            host_id: host_id.to_string(),
                        },
                        ActorId::from(host_id),
                        MESSAGE_DELAY,
                    );

                    for host in self.monitoring.borrow().get_schedulers_list() {
                        ctx.emit(
                            VMAllocationSucceeded {
                                vm: vm.clone(),
                                host_id: host_id.to_string(),
                            },
                            ActorId::from(&host),
                            MESSAGE_DELAY,
                        );
                    }
                } else {
                    info!(
                        "[time = {}] not enough space for vm #{} on host #{} in placement store",
                        ctx.time(),
                        vm.id,
                        host_id
                    );
                    ctx.emit(
                        ReportAllocationFailure {
                            vm: vm.clone(),
                            host_id: host_id.to_string(),
                        },
                        from.clone(),
                        MESSAGE_DELAY,
                    );
                }
            }
            VMAllocationFailed { vm, host_id } => {
                self.global_store.remove_vm(vm, host_id);

                for scheduler in self.monitoring.borrow().get_schedulers_list() {
                    ctx.emit(
                        DropVMOnScheduler {
                            vm: vm.clone(),
                            host_id: host_id.to_string(),
                        },
                        ActorId::from(&scheduler),
                        MESSAGE_DELAY,
                    );
                }
            }
            VMFinished { vm, host_id } => {
                self.global_store.remove_vm(vm, host_id);

                for scheduler in self.monitoring.borrow().get_schedulers_list() {
                    ctx.emit(
                        DropVMOnScheduler {
                            vm: vm.clone(),
                            host_id: host_id.to_string(),
                        },
                        ActorId::from(&scheduler),
                        MESSAGE_DELAY,
                    );
                }
            }
            OnNewHostAdded { id, host } => {
                info!("[time = {}] new host #{} added to main placement store", ctx.time(), id);
                self.global_store.add_host(id.to_string(), host);

                for scheduler in self.monitoring.borrow().get_schedulers_list() {
                    ctx.emit(
                        ReplicateNewHost {
                            id: id.clone(),
                            host: host.clone(),
                        },
                        ActorId::from(&scheduler),
                        MESSAGE_DELAY,
                    );
                }
            }
            OnNewSchedulerAdded { scheduler_id } => {
                info!(
                    "[time = {}] new scheduler #{} added to main placement store",
                    ctx.time(),
                    scheduler_id
                );
                ctx.emit(
                    ReceiveSnapshot {
                        local_store: self.global_store.clone(),
                    },
                    scheduler_id.clone(),
                    MESSAGE_DELAY,
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
