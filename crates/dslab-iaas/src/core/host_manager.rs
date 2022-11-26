//! Host manager representing a physical machine.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{log_debug, log_trace};

use crate::core::common::AllocationVerdict;
use crate::core::config::SimulationConfig;
use crate::core::energy_meter::EnergyMeter;
use crate::core::events::allocation::{
    AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest, MigrationRequest,
};
use crate::core::events::monitoring::HostStateUpdate;
use crate::core::events::vm::{VMDeleted, VMStarted};
use crate::core::events::vm_api::VmStatusChanged;
use crate::core::power_model::HostPowerModel;
use crate::core::slav_metric::HostSLAVMetric;
use crate::core::vm::{VirtualMachine, VmStatus};
use crate::core::vm_api::VmAPI;

/// Represents a single physical machine or host for short, which possesses a certain amount of resources and performs
/// execution of VMs assigned to it by a scheduler. It models the main VM lifecycle stages such as creation, deletion
/// and migration, and reports the VM status changes to VM API component. Host manager periodically computes its
/// current load, as the sum of loads produced by currently running VMs, and reports it to the monitoring component.
/// Host manager also records the total energy consumption of the host computed using the power model
/// defined as a function of CPU load.
pub struct HostManager {
    pub id: u32,
    name: String,

    cpu_total: u32,
    cpu_allocated: u32,
    cpu_available: u32,

    memory_total: u64,
    memory_allocated: u64,
    memory_available: u64,

    cpu_overcommit: u32,
    memory_overcommit: u64,

    vms: HashSet<u32>,
    recently_added_vms: Vec<u32>,
    recently_removed_vms: Vec<u32>,
    recent_vm_status_changes: HashMap<u32, VmStatus>,
    energy_meter: EnergyMeter,
    monitoring_id: u32,
    placement_store_id: u32,
    vm_api: Rc<RefCell<VmAPI>>,

    allow_vm_overcommit: bool,
    power_model: HostPowerModel,
    slav_metric: Box<dyn HostSLAVMetric>,

    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl HostManager {
    // Creates new host with specified capacity.
    pub fn new(
        cpu_total: u32,
        memory_total: u64,
        monitoring_id: u32,
        placement_store_id: u32,
        vm_api: Rc<RefCell<VmAPI>>,
        allow_vm_overcommit: bool,
        power_model: HostPowerModel,
        slav_metric: Box<dyn HostSLAVMetric>,
        ctx: SimulationContext,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id: ctx.id(),
            name: ctx.name().to_string(),
            cpu_total,
            memory_total,
            cpu_allocated: 0,
            memory_allocated: 0,
            cpu_available: cpu_total,
            memory_available: memory_total,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            vms: HashSet::new(),
            recently_added_vms: Vec::new(),
            recently_removed_vms: Vec::new(),
            recent_vm_status_changes: HashMap::new(),
            energy_meter: EnergyMeter::new(),
            monitoring_id,
            placement_store_id,
            vm_api,
            allow_vm_overcommit,
            power_model,
            slav_metric,
            ctx,
            sim_config,
        }
    }

    /// Checks if incoming VM can be allocated on this host.
    fn can_allocate(&self, vm_id: u32) -> AllocationVerdict {
        let vm = self.vm_api.borrow().get_vm(vm_id).borrow().clone();
        if self.allow_vm_overcommit {
            return AllocationVerdict::Success;
        }
        if self.cpu_available < vm.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.memory_available < vm.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    /// Allocates new virtual machine, updates resource and energy consumption.
    fn allocate(&mut self, time: f64, vm_ref: Rc<RefCell<VirtualMachine>>) {
        let vm = vm_ref.borrow();
        if self.cpu_available < vm.cpu_usage {
            self.cpu_overcommit += vm.cpu_usage - self.cpu_available;
            self.cpu_available = 0;
            self.cpu_allocated = self.cpu_total;
        } else {
            self.cpu_available -= vm.cpu_usage;
            self.cpu_allocated += vm.cpu_usage;
        }
        if self.memory_available < vm.memory_usage {
            self.memory_overcommit += vm.memory_usage - self.memory_available;
            self.memory_available = 0;
            self.memory_allocated = self.memory_total;
        } else {
            self.memory_available -= vm.memory_usage;
            self.memory_allocated += vm.memory_usage;
        }
        self.recently_added_vms.push(vm.id);
        self.vms.insert(vm.id);
        let cpu_load = self.get_cpu_load(time);
        let power = self.get_power(time, cpu_load);
        self.energy_meter.update(time, power);
        self.slav_metric.update(time, cpu_load);
    }

    /// Releases resources when VM is deleted, updates resource and energy consumption.
    fn release(&mut self, time: f64, vm_id: u32) {
        let vm = self.vm_api.borrow().get_vm(vm_id).borrow().clone();
        if self.cpu_overcommit >= vm.cpu_usage {
            self.cpu_overcommit -= vm.cpu_usage;
        } else {
            self.cpu_available += vm.cpu_usage - self.cpu_overcommit;
            self.cpu_allocated -= vm.cpu_usage - self.cpu_overcommit;
            self.cpu_overcommit = 0;
        }

        if self.memory_overcommit >= vm.memory_usage {
            self.memory_overcommit -= vm.memory_usage;
        } else {
            self.memory_available += vm.memory_usage - self.memory_overcommit;
            self.memory_allocated -= vm.memory_usage - self.memory_overcommit;
            self.memory_overcommit = 0;
        }
        self.vms.remove(&vm.id);
        self.recently_removed_vms.push(vm.id);
        let cpu_load = self.get_cpu_load(time);
        let power = self.get_power(time, cpu_load);
        self.energy_meter.update(time, power);
        self.slav_metric.update(time, cpu_load);
    }

    /// Returns the total amount of allocated vCPUs.
    pub fn get_cpu_allocated(&self) -> f64 {
        self.cpu_allocated as f64
    }

    /// Returns the total amount of allocated memory.
    pub fn get_memory_allocated(&self) -> f64 {
        self.memory_allocated as f64
    }

    /// Returns the current CPU load (used/total) by summing the resource consumption of all active VMs on this host.
    pub fn get_cpu_load(&self, time: f64) -> f64 {
        let mut cpu_used = 0.;
        for vm_id in &self.vms {
            let vm = self.vm_api.borrow().get_vm(*vm_id).borrow().clone();
            cpu_used += vm.cpu_usage as f64 * vm.get_cpu_load(time);
        }
        return cpu_used / self.cpu_total as f64;
    }

    /// Returns the current memory load (used/total) by summing the resource consumption of all active VMs on this host.
    pub fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for vm_id in &self.vms {
            let vm = self.vm_api.borrow().get_vm(*vm_id).borrow().clone();
            memory_used += vm.memory_usage as f64 * vm.get_memory_load(time);
        }
        return memory_used / self.memory_total as f64;
    }

    /// Returns the current power consumption.
    pub fn get_power(&self, time: f64, cpu_load: f64) -> f64 {
        // CPU utilization is capped by 100%
        let cpu_util = cpu_load.min(1.);
        return self.power_model.get_power(time, cpu_util);
    }

    /// Returns the total energy consumption.
    pub fn get_energy_consumed(&mut self, time: f64) -> f64 {
        let cpu_load = self.get_cpu_load(time);
        let power = self.get_power(time, cpu_load);
        self.energy_meter.update(time, power);
        return self.energy_meter.energy_consumed();
    }

    /// Returns the total SLAV value.
    pub fn get_accumulated_slav(&mut self, time: f64) -> f64 {
        let cpu_load = self.get_cpu_load(time);
        self.slav_metric.update(time, cpu_load);
        self.slav_metric.value()
    }

    /// Processes allocation request, allocates resources to start new VM.
    fn on_allocation_request(&mut self, vm_id: u32) -> bool {
        if self.can_allocate(vm_id) == AllocationVerdict::Success {
            let vm = self.vm_api.borrow().get_vm(vm_id);
            let start_duration = vm.borrow().start_duration();
            self.allocate(self.ctx.time(), vm);
            self.recent_vm_status_changes.insert(vm_id, VmStatus::Initializing);
            log_debug!(self.ctx, "vm {} allocated on host {}", vm_id, self.name);
            self.ctx.emit_self(VMStarted { vm_id }, start_duration);
            true
        } else {
            log_debug!(self.ctx, "not enough space for vm {} on host {}", vm_id, self.name);
            self.ctx.emit(
                AllocationFailed {
                    vm_id,
                    host_id: self.id,
                },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
            false
        }
    }

    /// Processes migration request (as migration target), allocates resources to start new VM, updates VM status.
    fn on_migration_request(&mut self, source_host: u32, vm_id: u32) {
        if self.can_allocate(vm_id) == AllocationVerdict::Success {
            let vm = self.vm_api.borrow().get_vm(vm_id);
            let migration_duration = (vm.borrow().memory_usage as f64) / (self.sim_config.network_throughput as f64);
            let start_duration = vm.borrow().start_duration();

            self.allocate(self.ctx.time(), vm);
            log_debug!(
                self.ctx,
                "vm {} allocated on host {}, start migration",
                vm_id,
                self.name
            );
            self.recent_vm_status_changes.insert(vm_id, VmStatus::Migrating);

            self.ctx
                .emit_self(VMStarted { vm_id }, migration_duration + start_duration);
            self.ctx.emit(
                AllocationReleaseRequest {
                    vm_id,
                    is_migrating: true,
                },
                source_host,
                migration_duration,
            );
        } else {
            log_debug!(
                self.ctx,
                "not enough space for vm {} on host {}, migration failed",
                vm_id,
                self.name
            );
        }
    }

    /// Processes resource release request by scheduling deletion of corresponding VM.
    fn on_allocation_release_request(&mut self, vm_id: u32, is_migrating: bool) {
        if self.vms.contains(&vm_id) {
            log_debug!(self.ctx, "release resources from vm {} on host {}", vm_id, self.name);
            if !is_migrating {
                self.recent_vm_status_changes.insert(vm_id, VmStatus::Finished);
            }
            let vm = self.vm_api.borrow().get_vm(vm_id).borrow().clone();
            self.ctx.emit_self(VMDeleted { vm_id }, vm.stop_duration());
        } else {
            log_trace!(self.ctx, "do not release, probably VM was migrated to other host");
        }
    }

    /// Invoked upon VM startup, updates VM status and schedules VM release event according to its lifetime.
    fn on_vm_started(&mut self, vm_id: u32) {
        log_debug!(self.ctx, "vm {} started and running", vm_id);
        let vm = self.vm_api.borrow().get_vm(vm_id);
        let start_time = vm.borrow().start_time();

        if start_time != -1. {
            // reduce lifetime due to migration
            let new_lifetime = vm.borrow().lifetime() - (self.ctx.time() - start_time);
            vm.borrow_mut().set_lifetime(new_lifetime);
        }

        vm.borrow_mut().set_start_time(self.ctx.time());
        self.recent_vm_status_changes.insert(vm_id, VmStatus::Running);
        self.ctx.emit_self(
            AllocationReleaseRequest {
                vm_id,
                is_migrating: false,
            },
            vm.borrow().lifetime(),
        );
    }

    /// Invoked upon VM deletion to release the allocated resources and notify placement store.
    fn on_vm_deleted(&mut self, vm_id: u32) {
        if self.vms.contains(&vm_id) {
            log_debug!(self.ctx, "vm {} deleted", vm_id);
            self.release(self.ctx.time(), vm_id);
            self.ctx.emit(
                AllocationReleased {
                    vm_id,
                    host_id: self.id,
                },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
        }
    }

    /// Invoked periodically to report the current host state to Monitoring and VM status updates to VM API.
    fn send_host_state(&mut self) {
        log_trace!(self.ctx, "host #{} sends it`s data to monitoring", self.id);
        let time = self.ctx.time();
        let cpu_load = self.get_cpu_load(time);
        let power = self.get_power(time, cpu_load);
        self.energy_meter.update(time, power);
        self.slav_metric.update(time, cpu_load);

        self.ctx.emit(
            HostStateUpdate {
                host_id: self.id,
                cpu_load,
                memory_load: self.get_memory_load(time),
                recently_added_vms: mem::take(&mut self.recently_added_vms),
                recently_removed_vms: mem::take(&mut self.recently_removed_vms),
            },
            self.monitoring_id,
            self.sim_config.message_delay,
        );
        for (vm_id, status) in self.recent_vm_status_changes.drain() {
            self.ctx.emit(
                VmStatusChanged { vm_id, status },
                self.vm_api.borrow().get_id(),
                self.sim_config.message_delay,
            );
        }
        self.ctx.emit_self(SendHostState {}, self.sim_config.send_stats_period);
    }
}

#[derive(Serialize)]
pub struct SendHostState {}

impl EventHandler for HostManager {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { vm_id } => {
                self.on_allocation_request(vm_id);
            }
            MigrationRequest { source_host, vm_id } => {
                self.on_migration_request(source_host, vm_id);
            }
            AllocationReleaseRequest { vm_id, is_migrating } => {
                self.on_allocation_release_request(vm_id, is_migrating);
            }
            VMStarted { vm_id } => {
                self.on_vm_started(vm_id);
            }
            VMDeleted { vm_id } => {
                self.on_vm_deleted(vm_id);
            }
            SendHostState {} => {
                self.send_host_state();
            }
        })
    }
}
