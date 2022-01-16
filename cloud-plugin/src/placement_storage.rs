use log::info;
use std::collections::BTreeMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::host::AllocationVerdict;
use crate::host::TryAllocateVM as TryAllocateVMOnHost;
use crate::network::MESSAGE_DELAY;
use crate::scheduler::VMAllocationFailed as ReportAllocationFailure;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub cpu_available: u32,
    pub ram_available: u32,

    pub cpu_full: u32,
    pub ram_full: u32,

    pub vms: BTreeMap<String, VirtualMachine>,
}

impl HostInfo {
    pub fn new(cpu_full: u32, ram_full: u32) -> Self {
        Self {
            cpu_available: cpu_full,
            ram_available: ram_full,
            cpu_full,
            ram_full,
            vms: BTreeMap::new(),
        }
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PlacementStorage {
    hosts: BTreeMap<String, HostInfo>,
}

impl PlacementStorage {
    pub fn new() -> Self {
        Self {
            hosts: BTreeMap::new(),
        }
    }

    pub fn add_host(&mut self, id: String, cpu_full: u32, ram_full: u32) {
        self.hosts.insert(id, HostInfo::new(cpu_full, ram_full));
    }

    fn can_allocate(&self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
        if self.hosts[host_id].cpu_available < vm.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.hosts[host_id].ram_available < vm.ram_usage {
            return AllocationVerdict::NotEnoughRAM;
        }
        return AllocationVerdict::Success;
    }

    fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available -= vm.cpu_usage;
            host.ram_available -= vm.ram_usage;
            host.vms.insert(vm.id.clone(), vm.clone());
        });
    }

    fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available += vm.cpu_usage;
            host.ram_available += vm.ram_usage;
            host.vms.remove(&vm.id);
        });
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
                                                   requester: from,
                                                   host_id: host_id.to_string()
                            },
                            ActorId::from(host_id), MESSAGE_DELAY
                    );
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
                ctx.emit(ReportAllocationFailure { vm: vm.clone(), host_id: host_id.to_string() }, 
                         from.clone(), MESSAGE_DELAY);
            }
            VMFinished { vm, host_id } => {
                self.remove_vm(vm, host_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
