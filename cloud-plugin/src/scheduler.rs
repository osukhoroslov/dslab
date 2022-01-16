use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use log::info;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::monitoring::Monitoring;
use crate::placement_storage::TryAllocateVM as TryAllocateVmOnStorage;
use crate::virtual_machine::VirtualMachine;

use crate::network::MESSAGE_DELAY;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

#[derive(Debug, Clone)]
pub struct ReservedResources {
    cpu: u32,
    ram: u32,
    vms: BTreeMap<String, VirtualMachine>,
}

impl ReservedResources {
    pub fn new() -> Self {
        Self {
            cpu: 0,
            ram: 0,
            vms: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalReservations {
    hosts: BTreeMap<String, ReservedResources>,
}

impl LocalReservations {
    pub fn new() -> Self {
        Self {
            hosts: BTreeMap::new(),
        }
    }

    pub fn reserve(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.entry(host_id.to_string()).or_insert(ReservedResources::new());
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu += vm.cpu_usage;
            host.ram += vm.ram_usage;
            host.vms.insert(vm.id.clone(), vm.clone());
        });
    }

    pub fn cancel_reservation(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu -= vm.cpu_usage;
            host.ram -= vm.ram_usage;
            host.vms.remove(&vm.id);
        });
    }

    pub fn get_reserved(&self, host_id: &String) -> ReservedResources {
        if self.hosts.contains_key(host_id) {
            return self.hosts[host_id].clone();
        }
        return ReservedResources::new();
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
    placement_storage: ActorId,
    reservations: LocalReservations,
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>,
               placement_storage: ActorId) -> Self {
        Self {
            id,
            monitoring,
            placement_storage,
            reservations: LocalReservations::new()
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

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.monitoring.borrow().get_hosts_list() {
                    let host_state = self.monitoring.borrow().get_host_state(ActorId::from(host));

                    let mut cpu_available = host_state.cpu_available;
                    let mut ram_available = host_state.ram_available;
                    let local_reserved = self.reservations.get_reserved(host);
                    cpu_available -= local_reserved.cpu;
                    ram_available -= local_reserved.ram;

                    if cpu_available >= vm.cpu_usage && ram_available >= vm.ram_usage {
                        info!(
                            "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(),
                            self.id,
                            vm.id,
                            host
                        );
                        found = true;
                        self.reservations.reserve(vm, host);

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
                self.reservations.cancel_reservation(vm, host_id);
            }
            VMAllocationFailed { vm, host_id } => {
                self.reservations.cancel_reservation(vm, host_id);
                ctx.emit_now(FindHostToAllocateVM { vm: vm.clone() }, ctx.id.clone());
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
