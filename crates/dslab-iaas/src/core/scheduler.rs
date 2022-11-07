//! Component performing allocation of resources for new VMs.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_debug;

use crate::core::config::SimulationConfig;
use crate::core::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::core::events::vm_api::VmStatusChanged;
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
    vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl Scheduler {
    /// Creates scheduler with specified VM placement algorithm.
    pub fn new(
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        vm_api: Rc<RefCell<VmAPI>>,
        placement_store_id: u32,
        vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
        ctx: SimulationContext,
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
            sim_config,
        }
    }

    /// Adds host to local resource pool state.
    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    /// Processes VM allocation request by selecting a host for running the VM.
    ///
    /// Host selection is performed by invoking the configured VM placement algorithm.
    /// If a suitable host is found, the scheduler updates its local state with new allocation and tries to commit its
    /// decision in the placement store.
    /// If not suitable host is found, the request is rescheduled for retry after the configured period.
    fn on_allocation_request(&mut self, vm_id: u32) {
        let vm = self.vm_api.borrow().get_vm(vm_id).borrow().clone();
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);

        if self.ctx.time() > vm.allocation_start_time + self.sim_config.vm_allocation_timeout {
            self.ctx.emit(
                VmStatusChanged {
                    vm_id,
                    status: VmStatus::FailedToAllocate,
                },
                self.vm_api.borrow().get_id(),
                self.sim_config.message_delay,
            );
            return;
        }

        if let Some(host) = self
            .vm_placement_algorithm
            .select_host(&alloc, &self.pool_state, &self.monitoring.borrow())
        {
            log_debug!(
                self.ctx,
                "decided to place vm {} on host {}",
                alloc.id,
                self.ctx.lookup_name(host)
            );
            self.pool_state.allocate(&alloc, host);

            self.ctx.emit(
                AllocationCommitRequest { vm_id, host_id: host },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
        } else {
            log_debug!(self.ctx, "failed to place vm {}", vm_id);
            self.ctx
                .emit_self(AllocationRequest { vm_id }, self.sim_config.allocation_retry_period);
        }
    }

    /// Applies committed allocation to the local resource pool state.
    fn on_allocation_commit_succeeded(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.allocate(&alloc, host_id);
    }

    /// Removes allocation failed during commit from the local resource pool state.
    fn on_allocation_commit_failed(&mut self, vm_id: u32, host_id: u32) {
        let alloc = self.vm_api.borrow().get_vm_allocation(vm_id);
        self.pool_state.release(&alloc, host_id);
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
            AllocationRequest { vm_id } => {
                self.on_allocation_request(vm_id);
            }
            AllocationCommitSucceeded { vm_id, host_id } => {
                self.on_allocation_commit_succeeded(vm_id, host_id);
            }
            AllocationCommitFailed { vm_id, host_id } => {
                self.on_allocation_commit_failed(vm_id, host_id);
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
