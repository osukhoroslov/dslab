use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{log_debug, log_trace};

use crate::core::common::Allocation;
use crate::core::common::AllocationVerdict;
use crate::core::config::SimulationConfig;
use crate::core::energy_manager::EnergyManager;
use crate::core::events::allocation::{
    AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest, MigrationRequest,
};
use crate::core::events::monitoring::HostStateUpdate;
use crate::core::events::vm::{VMDeleted, VMStarted};
use crate::core::vm::{VirtualMachine, VmStatus};

pub struct HostManager {
    pub id: u32,

    cpu_total: u32,
    cpu_available: u32,

    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,

    cpu_overcommit: u32,
    memory_overcommit: u64,

    allow_vm_overcommit: bool,
    allocs: HashMap<u32, Allocation>,
    vms: HashMap<u32, VirtualMachine>,
    recently_added_vms: Vec<(Allocation, VirtualMachine)>,
    recently_removed_vms: Vec<u32>,
    recent_vm_status_changes: HashMap<u32, (VmStatus, f64)>,
    energy_manager: EnergyManager,
    monitoring_id: u32,
    placement_store_id: u32,

    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl HostManager {
    pub fn new(
        cpu_total: u32,
        memory_total: u64,
        monitoring_id: u32,
        placement_store_id: u32,
        allow_vm_overcommit: bool,
        ctx: SimulationContext,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id: ctx.id(),
            cpu_total,
            memory_total,
            cpu_available: cpu_total,
            memory_available: memory_total,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            allow_vm_overcommit,
            allocs: HashMap::new(),
            vms: HashMap::new(),
            recently_added_vms: Vec::new(),
            recently_removed_vms: Vec::new(),
            recent_vm_status_changes: HashMap::new(),
            energy_manager: EnergyManager::new(),
            monitoring_id,
            placement_store_id,
            ctx,
            sim_config,
        }
    }

    fn can_allocate(&self, alloc: &Allocation) -> AllocationVerdict {
        if self.allow_vm_overcommit {
            return AllocationVerdict::Success;
        }
        if self.cpu_available < alloc.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.memory_available < alloc.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    fn allocate(&mut self, time: f64, alloc: Allocation, vm: VirtualMachine) {
        if self.cpu_available < alloc.cpu_usage {
            self.cpu_overcommit += alloc.cpu_usage - self.cpu_available;
            self.cpu_available = 0;
        } else {
            self.cpu_available -= alloc.cpu_usage;
        }
        if self.memory_available < alloc.memory_usage {
            self.memory_overcommit += alloc.memory_usage - self.memory_available;
            self.memory_available = 0;
        } else {
            self.memory_available -= alloc.memory_usage;
        }
        self.recently_added_vms.push((alloc.clone(), vm.clone()));
        self.vms.insert(alloc.id, vm);
        self.allocs.insert(alloc.id, alloc);
        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

    fn release(&mut self, time: f64, alloc_id: u32) -> Allocation {
        let alloc = self.allocs.remove(&alloc_id).unwrap();
        if self.cpu_overcommit >= alloc.cpu_usage {
            self.cpu_overcommit -= alloc.cpu_usage;
        } else {
            self.cpu_available += alloc.cpu_usage - self.cpu_overcommit;
            self.cpu_overcommit = 0;
        }

        if self.memory_overcommit >= alloc.memory_usage {
            self.memory_overcommit -= alloc.memory_usage;
        } else {
            self.memory_available += alloc.memory_usage - self.memory_overcommit;
            self.memory_overcommit = 0;
        }
        self.vms.remove(&alloc.id);
        self.recently_removed_vms.push(alloc.id);
        self.energy_manager.update_energy(time, self.get_energy_load(time));
        alloc
    }

    pub fn get_cpu_allocated(&self) -> f64 {
        let mut cpu_used = 0.;
        for (_vm_id, alloc) in &self.allocs {
            cpu_used += alloc.cpu_usage as f64;
        }
        return cpu_used;
    }

    pub fn get_memory_allocated(&self) -> f64 {
        let mut memory_used = 0.;
        for (_vm_id, alloc) in &self.allocs {
            memory_used += alloc.memory_usage as f64;
        }
        return memory_used;
    }

    pub fn get_cpu_load(&self, time: f64) -> f64 {
        let mut cpu_used = 0.;
        for (vm_id, alloc) in &self.allocs {
            cpu_used += alloc.cpu_usage as f64 * self.vms[vm_id].get_cpu_load(time);
        }
        return cpu_used / self.cpu_total as f64;
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for (vm_id, alloc) in &self.allocs {
            memory_used += alloc.memory_usage as f64 * self.vms[vm_id].get_memory_load(time);
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

    fn on_allocation_request(&mut self, alloc: Allocation, vm: VirtualMachine) -> bool {
        if self.can_allocate(&alloc) == AllocationVerdict::Success {
            let alloc_id = alloc.id;
            let start_duration = vm.start_duration();
            self.allocate(self.ctx.time(), alloc, vm);
            self.recent_vm_status_changes
                .insert(alloc_id, (VmStatus::Initializing, self.ctx.time()));
            log_debug!(self.ctx, "vm #{} allocated on host #{}", alloc_id, self.id);
            self.ctx.emit_self(VMStarted { id: alloc_id }, start_duration);
            true
        } else {
            log_debug!(self.ctx, "not enough space for vm #{} on host #{}", alloc.id, self.id);
            self.ctx.emit(
                AllocationFailed {
                    alloc,
                    host_id: self.id,
                },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
            false
        }
    }

    fn on_migration_request(&mut self, source_host: u32, alloc: Allocation, vm: VirtualMachine) {
        if self.can_allocate(&alloc) == AllocationVerdict::Success {
            let alloc_id = alloc.id;
            let migration_duration = (alloc.memory_usage as f64) / (self.sim_config.network_throughput as f64);
            let start_duration = vm.start_duration();

            self.allocate(self.ctx.time(), alloc, vm);
            log_debug!(
                self.ctx,
                "vm #{} allocated on host #{}, start migration",
                alloc_id,
                self.id
            );
            let local_vm = self.vms.get_mut(&alloc_id).unwrap();
            local_vm.set_status(VmStatus::Migrating);
            self.recent_vm_status_changes
                .insert(alloc_id, (VmStatus::Migrating, self.ctx.time()));

            self.ctx
                .emit_self(VMStarted { id: alloc_id }, migration_duration + start_duration);
            self.ctx
                .emit(AllocationReleaseRequest { alloc_id }, source_host, migration_duration);
        } else {
            log_debug!(
                self.ctx,
                "not enough space for vm #{} on host #{}, migration failed",
                alloc.id,
                self.id
            );
        }
    }

    fn on_allocation_release_request(&mut self, alloc_id: u32) {
        if self.allocs.contains_key(&alloc_id) {
            log_debug!(self.ctx, "release resources from vm #{} on host #{}", alloc_id, self.id);
            self.vms.get_mut(&alloc_id).unwrap().set_status(VmStatus::Finished);
            self.ctx.emit_self(
                VMDeleted { id: alloc_id },
                self.vms.get_mut(&alloc_id).unwrap().stop_duration(),
            );
        } else {
            log_trace!(self.ctx, "do not release, probably VM was migrated to other host");
        }
    }

    fn on_vm_started(&mut self, vm_id: u32) {
        log_debug!(self.ctx, "vm #{} started and running", vm_id);
        let vm = self.vms.get_mut(&vm_id).unwrap();
        vm.set_status(VmStatus::Running);
        vm.set_start_time(self.ctx.time());
        self.recent_vm_status_changes
            .insert(vm_id, (VmStatus::Running, self.ctx.time()));
        self.ctx.emit_self(
            AllocationReleaseRequest { alloc_id: vm_id },
            // keep lifetime correct after migrations!
            vm.lifetime() - (self.ctx.time() - vm.start_time()),
        );
    }

    fn on_vm_deleted(&mut self, vm_id: u32) {
        if self.vms.contains_key(&vm_id) {
            log_debug!(self.ctx, "vm #{} deleted", vm_id);
            let alloc = self.release(self.ctx.time(), vm_id);
            self.ctx.emit(
                AllocationReleased {
                    alloc,
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
                recent_vm_status_changes: mem::take(&mut self.recent_vm_status_changes),
            },
            self.monitoring_id,
            self.sim_config.message_delay,
        );

        self.ctx.emit_self(SendHostState {}, self.sim_config.send_stats_period);
    }
}

#[derive(Serialize)]
pub struct SendHostState {}

impl EventHandler for HostManager {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { alloc, vm } => {
                self.on_allocation_request(alloc, vm);
            }
            MigrationRequest { source_host, alloc, vm } => {
                self.on_migration_request(source_host, alloc, vm);
            }
            AllocationReleaseRequest { alloc_id } => {
                self.on_allocation_release_request(alloc_id);
            }
            VMStarted { id } => {
                self.on_vm_started(id);
            }
            VMDeleted { id } => {
                self.on_vm_deleted(id);
            }
            SendHostState {} => {
                self.send_host_state();
            }
        })
    }
}
