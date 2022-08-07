//! Central copy of cluster state database.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_debug;

use crate::core::common::AllocationVerdict;
use crate::core::config::SimulationConfig;
use crate::core::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm_api::VmAPI;

/// Central cluster state database, schedulers conflicts resolver.
pub struct PlacementStore {
    allow_vm_overcommit: bool,
    pool_state: ResourcePoolState,
    schedulers: HashSet<u32>,
    vm_api: Rc<RefCell<VmAPI>>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl PlacementStore {
    /// Create component.
    pub fn new(
        allow_vm_overcommit: bool,
        vm_api: Rc<RefCell<VmAPI>>,
        ctx: SimulationContext,
        sim_config: SimulationConfig,
    ) -> Self {
        Self {
            allow_vm_overcommit,
            pool_state: ResourcePoolState::new(),
            schedulers: HashSet::new(),
            vm_api,
            ctx,
            sim_config,
        }
    }

    /// Get component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Add new host to database.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    /// Subscribe new scheduler on system allocation events.
    pub fn add_scheduler(&mut self, id: u32) {
        self.schedulers.insert(id);
    }

    /// Get current cluster state in order to copy it to new scheduler.
    pub fn get_pool_state(&self) -> ResourcePoolState {
        self.pool_state.clone()
    }

    /// Try to apply scheduler decision. In case of conflict there are no enough space available.
    fn on_allocation_commit_request(&mut self, vm_id: u32, host_id: u32, from_scheduler: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        if self.allow_vm_overcommit || self.pool_state.can_allocate(&alloc, host_id) == AllocationVerdict::Success {
            self.pool_state.allocate(&alloc, host_id);
            log_debug!(
                self.ctx,
                "vm #{} commited to host #{} in placement store",
                vm_id,
                host_id
            );
            self.ctx
                .emit(AllocationRequest { vm_id }, host_id, self.sim_config.message_delay);

            for scheduler in self.schedulers.iter() {
                self.ctx.emit(
                    AllocationCommitSucceeded { vm_id, host_id },
                    *scheduler,
                    self.sim_config.message_delay,
                );
            }
        } else {
            log_debug!(
                self.ctx,
                "not enough space for vm #{} on host #{} in placement store",
                vm_id,
                host_id
            );
            self.ctx.emit(
                AllocationCommitFailed {
                    vm_id,
                    host_id: host_id,
                },
                from_scheduler,
                self.sim_config.message_delay,
            );
            self.ctx.emit(
                AllocationRequest { vm_id },
                from_scheduler,
                self.sim_config.message_delay + self.sim_config.allocation_retry_period,
            );
        }
    }

    /// If host allocation fails, usually that means host is overloaded.
    /// Allocate resources locally and inform all known schedulers.
    fn on_allocation_failed(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.release(&alloc, host_id);
        for scheduler in self.schedulers.iter() {
            self.ctx.emit(
                AllocationFailed { vm_id, host_id },
                *scheduler,
                self.sim_config.message_delay,
            );
        }
    }

    /// When VM lifecycle finishes - release resources locally and inform all known schedulers.
    fn on_allocation_released(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.release(&alloc, host_id);
        for scheduler in self.schedulers.iter() {
            self.ctx.emit(
                AllocationReleased { vm_id, host_id },
                *scheduler,
                self.sim_config.message_delay,
            );
        }
    }
}

impl EventHandler for PlacementStore {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationCommitRequest { vm_id, host_id } => {
                self.on_allocation_commit_request(vm_id, host_id, event.src)
            }
            AllocationFailed { vm_id, host_id } => {
                self.on_allocation_failed(vm_id, host_id)
            }
            AllocationReleased { vm_id, host_id } => {
                self.on_allocation_released(vm_id, host_id)
            }
        })
    }
}
