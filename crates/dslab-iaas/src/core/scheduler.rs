//! Component performing allocation of resources for new VMs.

use std::cell::RefCell;
use std::rc::Rc;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

use crate::core::common::Allocation;
use crate::core::config::sim_config::SimulationConfig;
use crate::core::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::core::events::vm_api::VmStatusChanged;
use crate::core::logger::Logger;
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm::VmStatus;
use crate::core::vm_api::VmAPI;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

/// Scheduler processes VM allocation requests by selecting hosts for running new VMs.
///
/// It stores a local copy of resource pool state, which includes current resource allocations on each host.
/// Scheduler can also access information about current load of each host from the monitoring component.
/// The actual VM placement decision is delegated to the configured VM placement algorithm.
///
/// It is possible to simulate a cloud with multiple schedulers that concurrently process allocation requests.
/// Since each scheduler operates using its own, possibly outdated resource pool state, the schedulers' decisions may
/// produce conflicts. For example, both schedulers have decided to place the corresponding VMs on the same host,
/// which cannot accommodate both of these VMs. The resolution of such conflicts and synchronization of scheduler
/// states is performed via `PlacementStore` component.
pub struct Scheduler {
    pub id: u32,
    pool_state: ResourcePoolState,
    monitoring: Rc<RefCell<Monitoring>>,
    vm_api: Rc<RefCell<VmAPI>>,
    placement_store_id: u32,
    vm_placement_algorithm: VMPlacementAlgorithm,
    ctx: SimulationContext,
    logger: Rc<RefCell<Box<dyn Logger>>>,
    sim_config: Rc<SimulationConfig>,
}

impl Scheduler {
    /// Creates scheduler with specified VM placement algorithm.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        vm_api: Rc<RefCell<VmAPI>>,
        placement_store_id: u32,
        vm_placement_algorithm: VMPlacementAlgorithm,
        ctx: SimulationContext,
        logger: Rc<RefCell<Box<dyn Logger>>>,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id: ctx.id(),
            pool_state: snapshot,
            monitoring,
            vm_api,
            placement_store_id,
            vm_placement_algorithm,
            ctx,
            logger,
            sim_config,
        }
    }

    /// Adds host to local resource pool state.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64, rack_id: Option<u32>) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total, rack_id);
    }

    /// Computes the placements (hosts) for a set of allocations using the configured placement algorithm.
    ///
    /// Returns None is it is not possible to satisfy all allocations.
    fn compute_placements(&mut self, allocations: &[Allocation]) -> Option<Vec<u32>> {
        match &self.vm_placement_algorithm {
            VMPlacementAlgorithm::Single(alg) => {
                if allocations.len() == 1 {
                    alg.select_host(&allocations[0], &self.pool_state, &self.monitoring.borrow())
                        .map(|h| vec![h])
                } else {
                    // schedule VMs from multi-VM request one-by-one
                    let mut result = Vec::new();
                    let mut pool_state_copy = self.pool_state.clone();
                    for alloc in allocations.iter() {
                        if let Some(host) = alg.select_host(alloc, &pool_state_copy, &self.monitoring.borrow()) {
                            pool_state_copy.allocate(alloc, host);
                            result.push(host);
                        } else {
                            return None;
                        }
                    }
                    Some(result)
                }
            }
            VMPlacementAlgorithm::Multi(alg) => {
                alg.select_hosts(allocations, &self.pool_state, &self.monitoring.borrow())
            }
        }
    }

    /// Processes allocation request by selecting host for running each VM.
    ///
    /// Host selection is performed by invoking the configured VM placement algorithm.
    /// If a suitable host is found, the scheduler updates its local state with new allocation and tries to commit its
    /// decision in the placement store.
    /// If a suitable host is not found, the request is rescheduled for retry after the configured period.
    fn on_allocation_request(&mut self, vm_ids: Vec<u32>) {
        // check if request is timed out
        let start_time = self.vm_api.borrow().get_vm(vm_ids[0]).borrow().allocation_start_time;
        if self.ctx.time() > start_time + self.sim_config.vm_allocation_timeout {
            for vm_id in vm_ids {
                self.ctx.emit(
                    VmStatusChanged {
                        vm_id,
                        status: VmStatus::FailedToAllocate,
                    },
                    self.vm_api.borrow().get_id(),
                    self.sim_config.message_delay,
                );
            }
            return;
        }

        let allocations: Vec<Allocation> = vm_ids
            .iter()
            .map(|vm_id| self.vm_api.borrow().get_vm_allocation(*vm_id))
            .collect();
        // try to find placements using the placement algorithm
        if let Some(placements) = self.compute_placements(&allocations) {
            for (host, alloc) in placements.iter().zip(allocations.iter()) {
                self.logger.borrow_mut().log_debug(
                    &self.ctx,
                    format!(
                        "decided to place vm {} on host {}",
                        alloc.id,
                        self.ctx.lookup_name(*host)
                    ),
                );
                self.pool_state.allocate(alloc, *host);
            }
            self.ctx.emit(
                AllocationCommitRequest {
                    vm_ids,
                    host_ids: placements,
                },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
        } else {
            self.logger
                .borrow_mut()
                .log_debug(&self.ctx, format!("failed to place {} vms", vm_ids.len()));
            self.ctx
                .emit_self(AllocationRequest { vm_ids }, self.sim_config.allocation_retry_period);
        }
    }

    /// Applies committed allocation to the local resource pool state.
    fn on_allocation_commit_succeeded(&mut self, vm_ids: Vec<u32>, host_ids: Vec<u32>) {
        for (&vm_id, &host_id) in vm_ids.iter().zip(host_ids.iter()) {
            let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
            self.pool_state.allocate(&alloc, host_id);
        }
    }

    /// Removes allocation failed during commit from the local resource pool state.
    fn on_allocation_commit_failed(&mut self, vm_ids: Vec<u32>, host_ids: Vec<u32>) {
        for (&vm_id, &host_id) in vm_ids.iter().zip(host_ids.iter()) {
            let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
            self.pool_state.release(&alloc, host_id);
        }
    }

    /// Removes released allocation from the local resource pool state.
    fn on_allocation_released(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.release(&alloc, host_id);
    }

    /// Removes failed allocation from the local resource pool state.
    fn on_allocation_failed(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.release(&alloc, host_id);
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { vm_ids } => {
                self.on_allocation_request(vm_ids);
            }
            AllocationCommitSucceeded { vm_ids, host_ids } => {
                self.on_allocation_commit_succeeded(vm_ids, host_ids);
            }
            AllocationCommitFailed { vm_ids, host_ids } => {
                self.on_allocation_commit_failed(vm_ids, host_ids);
            }
            AllocationReleased { vm_id, host_id } => {
                self.on_allocation_released(vm_id, host_id);
            }
            AllocationFailed { vm_id, host_id } => {
                self.on_allocation_failed(vm_id, host_id);
            }
        })
    }
}
