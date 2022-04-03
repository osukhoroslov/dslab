use std::cell::RefCell;
use std::rc::Rc;

use log::info;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

use crate::config::SimulationConfig;
use crate::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::monitoring::Monitoring;
use crate::resource_pool::{Allocation, ResourcePoolState};
use crate::vm::VirtualMachine;
use crate::vm_placement_algorithm::VMPlacementAlgorithm;

pub struct Scheduler {
    pub id: String,
    pool_state: ResourcePoolState,
    placement_store_id: String,
    monitoring: Rc<RefCell<Monitoring>>,
    vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl Scheduler {
    pub fn new(
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        placement_store_id: String,
        vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
        ctx: SimulationContext,
        sim_config: SimulationConfig,
    ) -> Self {
        Self {
            id: ctx.id().to_string(),
            pool_state: snapshot,
            placement_store_id,
            monitoring,
            vm_placement_algorithm,
            ctx,
            sim_config: sim_config.clone(),
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    fn on_allocation_request(&mut self, alloc: Allocation, vm: VirtualMachine) {
        if let Some(host) = self
            .vm_placement_algorithm
            .select_host(&alloc, &self.pool_state, &self.monitoring.borrow())
        {
            log_debug!(
                self.ctx,
                "scheduler #{} decided to pack vm #{} on host #{}",
                self.id,
                alloc.id,
                host
            );
            self.pool_state.allocate(&alloc, &host);

            self.ctx.emit(
                AllocationCommitRequest {
                    alloc,
                    vm,
                    host_id: host,
                },
                &self.placement_store_id,
                self.sim_config.message_delay,
            );
        } else {
            log_debug!(self.ctx, "scheduler #{} failed to pack vm #{}", self.id, alloc.id,);
            self.ctx
                .emit_self(AllocationRequest { alloc, vm }, self.sim_config.allocation_retry_period);
        }
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
