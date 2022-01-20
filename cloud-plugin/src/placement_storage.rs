use log::info;

use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::host::AllocationVerdict;
use crate::host::TryAllocateVM as TryAllocateVMOnHost;
use crate::monitoring::Monitoring;
use crate::network::MESSAGE_DELAY;
use crate::scheduler::VMAllocationSucceeded;
use crate::scheduler::VMAllocationFailed as ReportAllocationFailure;
use crate::scheduler::VMFinished as DropVMOnScheduler;
use crate::storage::Storage;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct PlacementStorage {
    storage: Storage,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl PlacementStorage {
    pub fn new(monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            storage: Storage::new(monitoring.clone()),
            monitoring: monitoring.clone()
        }
    }

    pub fn add_host(&mut self, id: String, cpu_full: u32, ram_full: u32) {
        self.storage.add_host(id.clone(), cpu_full, ram_full);
    }

    fn can_allocate(&mut self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
        self.storage.can_allocate(&vm, &host_id)
    }

    fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.storage.place_vm(&vm, &host_id);
    }

    fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.storage.remove_vm(&vm, &host_id);
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TryAllocateVM {
    pub vm: VirtualMachine,
    pub host_id: String
}

#[derive(Debug)]
pub struct VMAllocationFailed {
    pub vm: VirtualMachine,
    pub host_id: String
}

#[derive(Debug)]
pub struct VMFinished {
    pub vm: VirtualMachine,
    pub host_id: String
}

impl Actor for PlacementStorage {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            TryAllocateVM { vm, host_id } => {
                if self.can_allocate(vm, host_id) == AllocationVerdict::Success {
                    self.place_vm(vm, host_id);
                    info!("[time = {}] vm #{} commited to host #{} in placement storage",
                        ctx.time(), vm.id, host_id
                    );
                    ctx.emit(TryAllocateVMOnHost { vm: vm.clone(),
                                                   host_id: host_id.to_string()
                            },
                            ActorId::from(host_id), MESSAGE_DELAY
                    );

                    for host in self.monitoring.borrow().get_schedulers_list() {
                        ctx.emit(VMAllocationSucceeded { vm: vm.clone(),
                                                         host_id: host_id.to_string()
                            },
                            ActorId::from(&host), MESSAGE_DELAY
                        );
                    }
                } else {
                    info!(
                        "[time = {}] not enough space for vm #{} on host #{} in placement storage",
                        ctx.time(),
                        vm.id,
                        host_id
                    );
                    ctx.emit(ReportAllocationFailure { vm: vm.clone(), host_id: host_id.to_string() }, 
                            from.clone(), MESSAGE_DELAY);
                }
            }
            VMAllocationFailed { vm, host_id } => {
                self.remove_vm(vm, host_id);

                for scheduler in self.monitoring.borrow().get_schedulers_list() {
                    ctx.emit(DropVMOnScheduler { vm: vm.clone(),
                                          host_id: host_id.to_string()
                        },
                        ActorId::from(&scheduler), MESSAGE_DELAY
                    );
                }
            }
            VMFinished { vm, host_id } => {
                self.remove_vm(vm, host_id);

                for scheduler in self.monitoring.borrow().get_schedulers_list() {
                    ctx.emit(DropVMOnScheduler { vm: vm.clone(),
                                                 host_id: host_id.to_string()
                        },
                        ActorId::from(&scheduler), MESSAGE_DELAY
                    );
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
