use core::match_event;
use core::actor::{ActorId, Actor, ActorContext, Event};

use std::rc::Rc;
use std::cell::RefCell;

use crate::host::TryAllocateVM;
use crate::monitoring::Monitoring;
use crate::virtual_machine::VirtualMachine;

use crate::network::MESSAGE_DELAY;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            id,
            monitoring,
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.monitoring.borrow().get_hosts_list() {
                    let host_state = self.monitoring.borrow().get_host_state(ActorId::from(host));
                    let cpu_available = host_state.cpu_available;
                    let ram_available = host_state.ram_available;

                    if cpu_available >= vm.cpu_usage && ram_available >= vm.ram_usage {
                        println!("[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(), self.id, vm.id, ActorId::from(host)
                        );
                        found = true;
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
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
