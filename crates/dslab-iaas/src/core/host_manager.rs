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
use crate::core::energy_manager::EnergyManager;
use crate::core::events::allocation::{
    AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest, MigrationRequest,
};
use crate::core::events::monitoring::HostStateUpdate;
use crate::core::events::vm::{VMDeleted, VMStarted};
use crate::core::events::vm_api::VmStatusChanged;
use crate::core::vm::{VirtualMachine, VmStatus};
use crate::core::vm_api::VmAPI;

pub struct HostManager {
    pub id: u32,

    cpu_total: u32,
    cpu_allocated: u32,
    cpu_available: u32,

    #[allow(dead_code)]
    memory_total: u64,
    memory_allocated: u64,
    memory_available: u64,

    cpu_overcommit: u32,
    memory_overcommit: u64,

    allow_vm_overcommit: bool,

    vms: HashSet<u32>,
    recently_added_vms: Vec<u32>,
    recently_removed_vms: Vec<u32>,
    recent_vm_status_changes: HashMap<u32, (VmStatus, f64)>,
    energy_manager: EnergyManager,
    monitoring_id: u32,
    placement_store_id: u32,
    vm_api: Rc<RefCell<VmAPI>>,
    vm_api_id: u32,

    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl HostManager {
    pub fn new(
        cpu_total: u32,
        memory_total: u64,
        monitoring_id: u32,
        placement_store_id: u32,
        vm_api: Rc<RefCell<VmAPI>>,
        vm_api_id: u32,
        allow_vm_overcommit: bool,
        ctx: SimulationContext,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id: ctx.id(),
            cpu_total,
            memory_total,
            cpu_allocated: 0,
            memory_allocated: 0,
            cpu_available: cpu_total,
            memory_available: memory_total,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            allow_vm_overcommit,
            vms: HashSet::new(),
            recently_added_vms: Vec::new(),
            recently_removed_vms: Vec::new(),
            recent_vm_status_changes: HashMap::new(),
            energy_manager: EnergyManager::new(),
            monitoring_id,
            placement_store_id,
            vm_api,
            vm_api_id,
            ctx,
            sim_config,
        }
    }

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
        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

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
        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

    pub fn get_cpu_allocated(&self) -> f64 {
        self.cpu_allocated as f64
    }

    pub fn get_memory_allocated(&self) -> f64 {
        self.memory_allocated as f64
    }

    pub fn get_cpu_load(&self, time: f64) -> f64 {
        let mut cpu_used = 0.;
        for vm_id in &self.vms {
            let vm = self.vm_api.borrow().get_vm(*vm_id).borrow().clone();
            cpu_used += vm.cpu_usage as f64 * vm.get_cpu_load(time);
        }
        return cpu_used / self.cpu_total as f64;
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for vm_id in &self.vms {
            let vm = self.vm_api.borrow().get_vm(*vm_id).borrow().clone();
            memory_used += vm.memory_usage as f64 * vm.get_memory_load(time);
        }
        return memory_used / self.memory_total as f64;
    }

    pub fn get_energy_load(&self, time: f64) -> f64 {
        let cpu_load = self.get_cpu_load(time);
        if cpu_load == 0. {
            return 0.;
        }
        return 0.4 + 0.6 * cpu_load;
    }

    pub fn get_total_consumed(&mut self, time: f64) -> f64 {
        self.energy_manager.update_energy(time, self.get_energy_load(time));
        return self.energy_manager.get_total_consumed();
    }

    fn on_allocation_request(&mut self, vm_id: u32) -> bool {
        if self.can_allocate(vm_id) == AllocationVerdict::Success {
            let vm = self.vm_api.borrow().get_vm(vm_id);
            let start_duration = vm.borrow().start_duration();
            self.allocate(self.ctx.time(), vm.clone());
            self.recent_vm_status_changes
                .insert(vm_id, (VmStatus::Initializing, self.ctx.time()));
            log_debug!(self.ctx, "vm #{} allocated on host #{}", vm_id, self.id);
            self.ctx.emit_self(VMStarted { vm_id }, start_duration);
            true
        } else {
            log_debug!(self.ctx, "not enough space for vm #{} on host #{}", vm_id, self.id);
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

    fn on_migration_request(&mut self, source_host: u32, vm_id: u32) {
        if self.can_allocate(vm_id) == AllocationVerdict::Success {
            let vm = self.vm_api.borrow().get_vm(vm_id);
            let migration_duration = (vm.borrow().memory_usage as f64) / (self.sim_config.network_throughput as f64);
            let start_duration = vm.borrow().start_duration();

            self.allocate(self.ctx.time(), vm);
            log_debug!(
                self.ctx,
                "vm #{} allocated on host #{}, start migration",
                vm_id,
                self.id
            );
            self.recent_vm_status_changes
                .insert(vm_id, (VmStatus::Migrating, self.ctx.time()));

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
                "not enough space for vm #{} on host #{}, migration failed",
                vm_id,
                self.id
            );
        }
    }

    fn on_allocation_release_request(&mut self, vm_id: u32, is_migrating: bool) {
        if self.vms.contains(&vm_id) {
            log_debug!(self.ctx, "release resources from vm #{} on host #{}", vm_id, self.id);
            if !is_migrating {
                self.recent_vm_status_changes
                    .insert(vm_id, (VmStatus::Finished, self.ctx.time()));
            }
            let vm = self.vm_api.borrow().get_vm(vm_id).borrow().clone();
            self.ctx.emit_self(VMDeleted { vm_id }, vm.stop_duration());
        } else {
            log_trace!(self.ctx, "do not release, probably VM was migrated to other host");
        }
    }

    fn on_vm_started(&mut self, vm_id: u32) {
        log_debug!(self.ctx, "vm #{} started and running", vm_id);
        let vm = self.vm_api.borrow().get_vm(vm_id);
        let start_time = vm.borrow().start_time();

        if start_time != -1. {
            // reduce lifetime due to migration
            let new_lifetime = vm.borrow().lifetime() - (self.ctx.time() - start_time);
            vm.borrow_mut().set_lifetime(new_lifetime);
        }

        vm.borrow_mut().set_start_time(self.ctx.time());
        self.recent_vm_status_changes
            .insert(vm_id, (VmStatus::Running, self.ctx.time()));
        self.ctx.emit_self(
            AllocationReleaseRequest {
                vm_id,
                is_migrating: false,
            },
            vm.borrow().lifetime(),
        );
    }

    fn on_vm_deleted(&mut self, vm_id: u32) {
        if self.vms.contains(&vm_id) {
            log_debug!(self.ctx, "vm #{} deleted", vm_id);
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

    fn send_host_state(&mut self) {
        log_trace!(self.ctx, "host #{} sends it`s data to monitoring", self.id);
        self.energy_manager
            .update_energy(self.ctx.time(), self.get_energy_load(self.ctx.time()));

        self.ctx.emit(
            HostStateUpdate {
                host_id: self.id,
                cpu_load: self.get_cpu_load(self.ctx.time()),
                memory_load: self.get_memory_load(self.ctx.time()),
                recently_added_vms: mem::take(&mut self.recently_added_vms),
                recently_removed_vms: mem::take(&mut self.recently_removed_vms),
            },
            self.monitoring_id,
            self.sim_config.message_delay,
        );
        for (vm_id, (status, _)) in self.recent_vm_status_changes.drain() {
            self.ctx.emit(
                VmStatusChanged { vm_id, status },
                self.vm_api_id,
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
