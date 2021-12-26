use core::cast;
use core::actor::{ActorId, Actor, ActorContext, Event};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::host::TryAllocateVM;
use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::virtual_machine::VirtualMachine;

use crate::network::MESSAGE_DELAY;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
    reservations: HashMap<String, HashMap<String, VirtualMachine>>,
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            id,
            monitoring,
            reservations: HashMap::new(),
        }
    }

    fn get_host_estimated_state(&self, id: &str) -> HostState {
        let real_state = self.monitoring.borrow().get_host_state(ActorId::from(id));
        let mut estimated = real_state;

        if self.reservations.contains_key(id) {
            for vm in self.reservations[id].values() {
                if vm.cpu_usage < estimated.cpu_available {
                    estimated.cpu_available -= vm.cpu_usage;
                } else {
                    estimated.cpu_available = 0;
                }
                if vm.ram_usage < estimated.ram_available {
                    estimated.ram_available -= vm.ram_usage;
                } else {
                    estimated.ram_available = 0;
                }
            }
        }
        return estimated;
    }

}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine
}

#[derive(Debug, Clone)]
pub struct UndoReservation {
    pub host_id: String,
    pub vm_id: String,
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.monitoring.borrow().get_hosts_list() {
                    let host_state = self.get_host_estimated_state(host);
                    let cpu_available = host_state.cpu_available;
                    let ram_available = host_state.ram_available;

                    if cpu_available >= vm.cpu_usage && ram_available >= vm.ram_usage {
                        println!("[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(), self.id, vm.id, ActorId::from(host)
                        );
                        found = true;
                        if !self.reservations.contains_key(host) {
                            self.reservations.insert(host.to_string(), HashMap::new());
                        }
                        self.reservations.get_mut(host).unwrap().insert(vm.id.clone(), vm.clone());

                        ctx.emit(TryAllocateVM { vm: vm.clone() },
                            ActorId::from(host),
                            MESSAGE_DELAY
                        );
                        break;
                    }
                }
                if !found {
                    println!("[time = {}] scheduler #{} failed to pack vm #{}",
                        ctx.time(), self.id, vm.id);

                    ctx.emit(FindHostToAllocateVM { vm: vm.clone() },
                            ctx.id.clone(),
                            ALLOCATION_RETRY_PERIOD
                    );
                }
            }
            UndoReservation { host_id, vm_id } => {
                self.reservations.get_mut(host_id).unwrap().remove(vm_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
