use core::match_event;
use core::actor::{ActorId, Actor, ActorContext, Event};

use std::rc::Rc;
use std::cell::RefCell;

use crate::host::TryAllocateVM;
use crate::monitoring::Monitoring;
use crate::virtual_machine::VirtualMachine;

use crate::host::ALLOCATION_RETRY_PERIOD;
use crate::host::MESSAGE_DELAY;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct AllocationAgent {
    pub id: ActorId,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl AllocationAgent {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            id: id,
            monitoring: monitoring,
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine
}

impl Actor for AllocationAgent {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for i in 0..self.monitoring.borrow().number_of_hosts() {
                    let cpu_available = self.monitoring.borrow().cpu_available(i);
                    let ram_available = self.monitoring.borrow().ram_available(i);

                    if cpu_available >= vm.cpu_usage && ram_available >= vm.ram_usage {
                        println!("[time = {}] balancer #{} decided to pack vm #{} on host #{}",
                            ctx.time(), self.id, vm.id,
                            self.monitoring.borrow().get_host_actor_id(i)
                        );
                        found = true;
                        ctx.emit(TryAllocateVM { vm: vm.clone() },
                            self.monitoring.borrow().get_host_actor_id(i).clone(),
                            MESSAGE_DELAY
                        );
                        break;
                    }
                }
                if !found {
                    println!("[time = {}] balancer #{} failed to pack vm #{}",
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
