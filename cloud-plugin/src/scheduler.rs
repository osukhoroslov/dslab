use log::info;
use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::common::AllocationVerdict;
use crate::events::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationReleased, AllocationRequest,
};
use crate::monitoring::Monitoring;
use crate::network::MESSAGE_DELAY;
use crate::resource_pool::ResourcePoolState;
use crate::vm::VirtualMachine;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub id: ActorId,
    pool_state: ResourcePoolState,
    placement_store: ActorId,
    #[allow(dead_code)]
    monitoring: Rc<RefCell<Monitoring>>,
}

impl Scheduler {
    pub fn new(
        id: ActorId,
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        placement_store: ActorId,
    ) -> Self {
        Self {
            id,
            pool_state: snapshot,
            placement_store,
            monitoring,
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    fn on_allocation_request(&mut self, vm: &VirtualMachine, ctx: &mut ActorContext) {
        // pack via First Fit policy
        let mut found = false;
        for host in self.pool_state.get_hosts_list() {
            if self.pool_state.can_allocate(&vm, &host) == AllocationVerdict::Success {
                info!(
                    "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                    ctx.time(),
                    self.id,
                    vm.id,
                    host
                );
                found = true;
                self.pool_state.place_vm(&vm, &host);

                ctx.emit(
                    AllocationCommitRequest {
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

            ctx.emit_self(AllocationRequest { vm: vm.clone() }, ALLOCATION_RETRY_PERIOD);
        }
    }

    fn on_allocation_commit_succeeded(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.pool_state.place_vm(vm, host_id);
    }

    fn on_allocation_commit_failed(&mut self, vm: &VirtualMachine, host_id: &String, ctx: &mut ActorContext) {
        self.pool_state.remove_vm(vm, host_id);
        ctx.emit_now(AllocationRequest { vm: vm.clone() }, ctx.id.clone());
    }

    fn on_allocation_released(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.pool_state.remove_vm(vm, host_id);
    }
}

impl Actor for Scheduler {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            AllocationRequest { vm } => {
                self.on_allocation_request(vm, ctx);
            }
            AllocationCommitSucceeded { vm, host_id } => {
                self.on_allocation_commit_succeeded(vm, host_id);
            }
            AllocationCommitFailed { vm, host_id } => {
                self.on_allocation_commit_failed(vm, host_id, ctx);
            }
            AllocationReleased { vm, host_id } => {
                self.on_allocation_released(vm, host_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
