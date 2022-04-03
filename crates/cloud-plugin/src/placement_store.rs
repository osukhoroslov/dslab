use std::collections::HashSet;
use std::cell::RefCell;
use std::rc::Rc;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

use crate::common::AllocationVerdict;
use crate::config::SimulationConfig;
use crate::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::resource_pool::{Allocation, ResourcePoolState};
use crate::vm::VirtualMachine;

pub struct PlacementStore {
    allow_vm_overcommit: bool,
    pool_state: ResourcePoolState,
    schedulers: HashSet<String>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl PlacementStore {
    pub fn new(allow_vm_overcommit: bool, ctx: SimulationContext, sim_config: SimulationConfig) -> Self {
        Self {
            allow_vm_overcommit,
            pool_state: ResourcePoolState::new(),
            schedulers: HashSet::new(),
            ctx,
            sim_config: sim_config.clone(),
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    pub fn add_scheduler(&mut self, id: &str) {
        self.schedulers.insert(id.to_string());
    }

    pub fn get_pool_state(&self) -> ResourcePoolState {
        self.pool_state.clone()
    }

    fn on_allocation_commit_request(
        &mut self,
        alloc: Allocation,
        vm: VirtualMachine,
        host_id: String,
        from_scheduler: String,
    ) {
        if self.allow_vm_overcommit || self.pool_state.can_allocate(&alloc, &host_id) == AllocationVerdict::Success {
            self.pool_state.allocate(&alloc, &host_id);
            log_debug!(
                self.ctx,
                "vm #{} commited to host #{} in placement store",
                alloc.id,
                host_id
            );
            self.ctx.emit(
                AllocationRequest {
                    alloc: alloc.clone(),
                    vm,
                },
                &host_id,
                self.sim_config.message_delay,
            );

            for scheduler in self.schedulers.iter() {
                self.ctx.emit(
                    AllocationCommitSucceeded {
                        alloc: alloc.clone(),
                        host_id: host_id.clone(),
                    },
                    scheduler,
                    self.sim_config.message_delay,
                );
            }
        } else {
            log_debug!(
                self.ctx,
                "not enough space for vm #{} on host #{} in placement store",
                alloc.id,
                host_id
            );
            self.ctx.emit(
                AllocationCommitFailed {
                    alloc: alloc.clone(),
                    host_id: host_id.clone(),
                },
                &from_scheduler,
                self.sim_config.message_delay,
            );
            self.ctx.emit(
                AllocationRequest { alloc, vm },
                &from_scheduler,
                self.sim_config.message_delay + self.sim_config.allocation_retry_period,
            );
        }
    }

    fn on_allocation_failed(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.release(&alloc, &host_id);

        for scheduler in self.schedulers.iter() {
            self.ctx.emit(
                AllocationFailed {
                    alloc: alloc.clone(),
                    host_id: host_id.clone(),
                },
                scheduler,
                self.sim_config.message_delay,
            );
        }
    }

    fn on_allocation_released(&mut self, alloc: Allocation, host_id: String) {
        self.pool_state.release(&alloc, &host_id);

        for scheduler in self.schedulers.iter() {
            self.ctx.emit(
                AllocationReleased {
                    alloc: alloc.clone(),
                    host_id: host_id.clone(),
                },
                scheduler,
                self.sim_config.message_delay,
            );
        }
    }
}

impl EventHandler for PlacementStore {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationCommitRequest { alloc, vm, host_id } => {
                self.on_allocation_commit_request(alloc, vm, host_id, event.src)
            }
            AllocationFailed { alloc, host_id } => {
                self.on_allocation_failed(alloc, host_id)
            }
            AllocationReleased { alloc, host_id } => {
                self.on_allocation_released(alloc, host_id)
            }
        })
    }
}
