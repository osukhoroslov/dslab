use log::info;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationReleased, AllocationRequest,
};
use crate::monitoring::Monitoring;
use crate::network::MESSAGE_DELAY;
use crate::resource_pool::ResourcePoolState;
use crate::vm::VirtualMachine;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("").finish()
    }
}

#[derive(Clone)]
pub struct Scheduler {
    pub id: ActorId,
    pool_state: ResourcePoolState,
    placement_store: ActorId,
    #[allow(dead_code)]
    monitoring: Rc<RefCell<Monitoring>>,
    allocation_policy: fn(&VirtualMachine, &ResourcePoolState) -> Option<String>,
}

impl Scheduler {
    pub fn new(
        id: ActorId,
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        placement_store: ActorId,
        allocation_policy: fn(&VirtualMachine, &ResourcePoolState) -> Option<String>,
    ) -> Self {
        Self {
            id,
            pool_state: snapshot,
            placement_store,
            monitoring,
            allocation_policy,
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    pub fn on_allocation_host_found(&mut self, vm: &VirtualMachine, host: &String, ctx: &mut ActorContext) {
        info!(
            "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
            ctx.time(),
            self.id,
            vm.id,
            host
        );
        self.pool_state.place_vm(&vm, &host);
        ctx.emit(
            AllocationCommitRequest {
                vm: vm.clone(),
                host_id: host.to_string(),
            },
            self.placement_store.clone(),
            MESSAGE_DELAY,
        );
    }

    pub fn on_not_enough_space_available(&self, vm: &VirtualMachine, ctx: &mut ActorContext) {
        info!(
            "[time = {}] scheduler #{} failed to pack vm #{}",
            ctx.time(),
            self.id,
            vm.id
        );

        ctx.emit_self(AllocationRequest { vm: vm.clone() }, ALLOCATION_RETRY_PERIOD);
    }

    fn on_allocation_request(&mut self, vm: &VirtualMachine, ctx: &mut ActorContext) {
        let selected_host = (self.allocation_policy)(vm, &self.pool_state);
        if selected_host.is_none() {
            self.on_not_enough_space_available(vm, ctx);
        } else {
            self.on_allocation_host_found(vm, &selected_host.unwrap(), ctx);
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
