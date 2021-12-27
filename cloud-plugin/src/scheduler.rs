use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use log::info;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

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
    host_states: HashMap<String, HostState>,
}

impl Scheduler {
    pub fn new(id: ActorId, monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            id,
            monitoring,
            host_states: HashMap::new(),
        }
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine,
}

#[derive(Debug, Clone)]
pub struct VMAllocationFailed {
    pub vm: VirtualMachine,
}

#[derive(Debug, Clone)]
pub struct VMFinished {
    pub host_id: String,
    pub vm: VirtualMachine,
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for host in self.monitoring.borrow().get_hosts_list() {
                    let mut host_state = self.monitoring.borrow().get_host_state(ActorId::from(host));
                    if self.host_states.contains_key(host) {
                        host_state = self.host_states[host].clone();
                    }

                    let cpu_available = host_state.cpu_available;
                    let ram_available = host_state.ram_available;

                    if cpu_available >= vm.cpu_usage && ram_available >= vm.ram_usage {
                        info!(
                            "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                            ctx.time(),
                            self.id,
                            vm.id,
                            ActorId::from(host)
                        );
                        found = true;

                        self.host_states.entry(host.to_string()).or_insert(host_state);
                        self.host_states.get_mut(host).unwrap().cpu_available -= vm.cpu_usage;
                        self.host_states.get_mut(host).unwrap().ram_available -= vm.ram_usage;

                        ctx.emit(TryAllocateVM { vm: vm.clone() }, ActorId::from(host), MESSAGE_DELAY);
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
            VMAllocationFailed { vm } => {
                ctx.emit_now(FindHostToAllocateVM { vm: vm.clone() }, ctx.id.clone());
            }
            VMFinished { host_id, vm } => {
                self.host_states.get_mut(host_id).unwrap().cpu_available += vm.cpu_usage;
                self.host_states.get_mut(host_id).unwrap().ram_available += vm.ram_usage;
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
