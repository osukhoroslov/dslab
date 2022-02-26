use std::cell::RefCell;
use std::rc::Rc;

use log::info;

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;

use crate::common::AllocationVerdict;
use crate::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::monitoring::Monitoring;
use crate::network::MESSAGE_DELAY;
use crate::resource_pool::{Allocation, ResourcePoolState};
use crate::vm::VirtualMachine;

pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

pub struct Scheduler {
    pub id: String,
    pool_state: ResourcePoolState,
    placement_store_id: String,
    #[allow(dead_code)]
    monitoring: Rc<RefCell<Monitoring>>,
    ctx: SimulationContext,
}

impl Scheduler {
    pub fn new(
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        placement_store_id: String,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id().to_string(),
            pool_state: snapshot,
            placement_store_id,
            monitoring,
            ctx,
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    fn on_allocation_request(&mut self, alloc: Allocation, vm: VirtualMachine) {
        // pack via First Fit policy
        for host_id in self.pool_state.get_hosts_list() {
            if self.pool_state.can_allocate(&alloc, &host_id) == AllocationVerdict::Success {
                info!(
                    "[time = {}] scheduler #{} decided to pack vm #{} on host #{}",
                    self.ctx.time(),
                    self.id,
                    alloc.id,
                    host_id
                );
                self.pool_state.allocate(&alloc, &host_id);

                self.ctx.emit(
                    AllocationCommitRequest { alloc, vm, host_id },
                    &self.placement_store_id,
                    MESSAGE_DELAY,
                );
                return;
            }
        }
        info!(
            "[time = {}] scheduler #{} failed to pack vm #{}",
            self.ctx.time(),
            self.id,
            alloc.id,
        );
        self.ctx
            .emit_self(AllocationRequest { alloc, vm }, ALLOCATION_RETRY_PERIOD);
    }

    fn on_allocation_commit_succeeded(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.allocate(&alloc, &host_id);
    }

    fn on_allocation_commit_failed(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.release(&alloc, &host_id);
    }

    fn on_allocation_released(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.release(&alloc, &host_id);
    }

    fn on_allocation_failed(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.release(&alloc, &host_id);
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { alloc, vm } => {
                self.on_allocation_request(alloc, vm);
            }
            AllocationCommitSucceeded { alloc, host_id } => {
                self.on_allocation_commit_succeeded(alloc, host_id);
            }
            AllocationCommitFailed { alloc, host_id } => {
                self.on_allocation_commit_failed(alloc, host_id);
            }
            AllocationReleased { alloc, host_id } => {
                self.on_allocation_released(alloc, host_id);
            }
            AllocationFailed { alloc, host_id } => {
                self.on_allocation_failed(alloc, host_id);
            }
        })
    }
}
