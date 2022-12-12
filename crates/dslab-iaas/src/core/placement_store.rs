//! Component managing the authoritative copy of resource pool state.

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

/// This component maintains the authoritative copy of resource pool state and serializes updates to it.
///
/// Each scheduler sends its placement decisions to the placement store (PS), which checks each decision for possible
/// conflicts using its current pool state.
///
/// If no conflicts are detected, PS applies the change to its pool state and notifies about this change all
/// schedulers, which in turn update their local pool states. PS also passes the committed decision to the host which
/// has been selected for VM execution.
///
/// If a conflict is detected, PS rejects the update, notifies about it the corresponding scheduler and resubmits the
/// failed allocation request with configurable retry delay.
///
/// A user can configure the message delay for communication between schedulers and PS, which influences the staleness
/// of scheduler states and conflict rate.
pub struct PlacementStore {
    allow_vm_overcommit: bool,
    pool_state: ResourcePoolState,
    schedulers: HashSet<u32>,
    vm_api: Rc<RefCell<VmAPI>>,
    ctx: SimulationContext,
    sim_config: SimulationConfig,
}

impl PlacementStore {
    /// Creates component.
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

    /// Returns component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Adds new host to resource pool state.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    /// Registers scheduler so that PS can notify it about allocation events.
    pub fn add_scheduler(&mut self, id: u32) {
        self.schedulers.insert(id);
    }

    /// Returns a copy of the current resource pool state (e.g. to initialize a state of new scheduler).
    pub fn get_pool_state(&self) -> ResourcePoolState {
        self.pool_state.clone()
    }

    /// Processes direct allocation commit request bypassing the schedulers.
    pub fn direct_allocation_commit(&mut self, vm_id: u32, host_id: u32) {
        self.on_allocation_commit_request(vm_id, host_id, None);
    }

    /// Processes allocation commit requests from schedulers.
    fn on_allocation_commit_request(&mut self, vm_id: u32, host_id: u32, from_scheduler: Option<u32>) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        if self.allow_vm_overcommit || self.pool_state.can_allocate(&alloc, host_id) == AllocationVerdict::Success {
            self.pool_state.allocate(&alloc, host_id);
            log_debug!(
                self.ctx,
                "committed placement of vm {} to host {}",
                vm_id,
                self.ctx.lookup_name(host_id)
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
                "rejected placement of vm {} on host {} due to insufficient resources",
                vm_id,
                self.ctx.lookup_name(host_id)
            );
            if let Some(scheduler) = from_scheduler {
                self.ctx.emit(
                    AllocationCommitFailed { vm_id, host_id },
                    scheduler,
                    self.sim_config.message_delay,
                );
                self.ctx.emit(
                    AllocationRequest { vm_id },
                    scheduler,
                    self.sim_config.message_delay + self.sim_config.allocation_retry_period,
                );
            }
        }
    }

    /// Processes AllocationFailed events from host managers.
    ///
    /// If host allocation fails, usually that means that the host is overloaded.
    /// Updates the local state by releasing the corresponding resources and forwards this event to all schedulers.
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

    /// Processes AllocationReleased events that correspond to deletion of VM from the host.
    ///
    /// Updates the local state by releasing the corresponding resources and forwards this event to all schedulers.
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
                self.on_allocation_commit_request(vm_id, host_id, Some(event.src))
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
