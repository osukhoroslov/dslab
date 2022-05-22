use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

use serde::Serialize;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::{log_debug, log_trace};

use crate::core::common::AllocationVerdict;
use crate::core::common::VmStatus;
use crate::core::config::SimulationConfig;
use crate::core::energy_manager::EnergyManager;
use crate::core::events::allocation::{
    AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest, MigrationRequest,
};
use crate::core::events::monitoring::HostStateUpdate;
use crate::core::events::vm::{VMDeleted, VMStarted};
use crate::core::resource_pool::Allocation;
use crate::core::vm::VirtualMachine;

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
    recently_added_vms: Vec<u32>,
    recently_removed_vms: Vec<u32>,
    recent_vm_status_changes: HashMap<u32, VmStatus>,
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

    fn allocate(&mut self, time: f64, alloc: &Allocation, mut vm: VirtualMachine) {
        vm.set_creation_time(time);
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

        self.allocs.insert(alloc.id, alloc.clone());
        self.vms.insert(alloc.id, vm);
        self.recently_added_vms.push(alloc.id);
        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

    fn release(&mut self, time: f64, alloc: &Allocation) {
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
        self.allocs.remove(&alloc.id);
        self.vms.remove(&alloc.id);
        self.recently_removed_vms.push(alloc.id);
        self.energy_manager.update_energy(time, self.get_energy_load(time));
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
            if self.vms.get(&alloc.id).unwrap().status != VmStatus::Running {
                continue;
            }
            cpu_used += alloc.cpu_usage as f64 * self.vms[vm_id].get_cpu_load(time);
        }
        return cpu_used / self.cpu_total as f64;
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for (vm_id, alloc) in &self.allocs {
            memory_used += alloc.memory_usage as f64 * self.vms[vm_id].get_cpu_load(time);
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
            let start_duration = vm.start_duration();
            self.allocate(self.ctx.time(), &alloc, vm);
            log_debug!(self.ctx, "vm #{} allocated on host #{}", alloc.id, self.id);
            self.ctx.emit_self(VMStarted { alloc }, start_duration);
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
            let start_duration = vm.start_duration();
            self.allocate(self.ctx.time(), &alloc, vm);
            log_debug!(
                self.ctx,
                "vm #{} allocated on host #{}, start migration",
                alloc.id,
                self.id
            );
            let local_vm = self.vms.get_mut(&alloc.id).unwrap();
            local_vm.set_new_status(VmStatus::Migrating);
            self.recent_vm_status_changes.insert(alloc.id, VmStatus::Migrating);

            let migration_duration = (alloc.memory_usage as f64) / (self.sim_config.network_throughput as f64);

            self.ctx
                .emit_self(VMStarted { alloc: alloc.clone() }, migration_duration + start_duration);
            self.ctx.emit(
                AllocationReleaseRequest { alloc: alloc.clone() },
                source_host,
                migration_duration,
            );
        } else {
            log_debug!(
                self.ctx,
                "not enough space for vm #{} on host #{}, migration failed",
                alloc.id,
                self.id
            );
        }
    }

    fn on_allocation_release_request(&mut self, alloc: Allocation) {
        log_debug!(self.ctx, "release resources from vm #{} on host #{}", alloc.id, self.id);
        if self.vms.get(&alloc.id).is_none() {
            log_debug!(self.ctx, "do not release, probably VM was migrated to other host");
            return;
        }

        self.vms
            .get_mut(&alloc.id)
            .unwrap()
            .set_new_status(VmStatus::Deactivated);
        self.ctx.emit_self(
            VMDeleted { alloc: alloc.clone() },
            self.vms.get_mut(&alloc.id).unwrap().stop_duration(),
        );
    }

    fn on_vm_started(&mut self, alloc: Allocation) {
        log_debug!(self.ctx, "vm #{} started and running", alloc.id);
        self.vms.get_mut(&alloc.id).unwrap().set_new_status(VmStatus::Running);
        self.vms.get_mut(&alloc.id).unwrap().set_start_time(self.ctx.time());
        self.recent_vm_status_changes.insert(alloc.id, VmStatus::Running);

        self.ctx.emit_self(
            AllocationReleaseRequest { alloc: alloc.clone() },
            self.vms.get(&alloc.id).unwrap().lifetime(),
        );
    }

    fn on_vm_deleted(&mut self, alloc: Allocation) {
        log_debug!(self.ctx, "vm #{} deleted", alloc.id);
        self.release(self.ctx.time(), &alloc);
        self.ctx.emit(
            AllocationReleased {
                alloc,
                host_id: self.id,
            },
            self.placement_store_id,
            self.sim_config.message_delay,
        );
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
            AllocationReleaseRequest { alloc } => {
                self.on_allocation_release_request(alloc);
            }
            VMStarted { alloc } => {
                self.on_vm_started(alloc);
            }
            VMDeleted { alloc } => {
                self.on_vm_deleted(alloc);
            }
            SendHostState {} => {
                self.send_host_state();
            }
        })
    }
}
