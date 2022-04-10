use std::collections::HashMap;
use std::rc::Rc;

use serde::Serialize;

use simcore::cast;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::log_debug;

use crate::common::AllocationVerdict;
use crate::config::SimulationConfig;
use crate::energy_manager::EnergyManager;
use crate::events::allocation::{
    AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest, MigrationRequest,
};
use crate::events::monitoring::HostStateUpdate;
use crate::events::vm::{VMDeleted, VMStarted};
use crate::resource_pool::Allocation;
use crate::vm::VirtualMachine;

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
    previously_added_vms: Vec<u32>,
    previously_removed_vms: Vec<u32>,
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
            previously_added_vms: Vec::new(),
            previously_removed_vms: Vec::new(),
            energy_manager: EnergyManager::new(),
            monitoring_id,
            placement_store_id,
            ctx,
            sim_config: sim_config.clone(),
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
        vm.set_start_time(time);
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
        self.previously_added_vms.push(alloc.id);
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
        self.previously_removed_vms.push(alloc.id);
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
        let vm = self.vms.get(&alloc.id);
        if vm.is_none() {
            log_debug!(self.ctx, "do not release, probably VM was migrated to other host");
            return;
        }

        self.ctx.emit_self(VMDeleted { alloc }, vm.unwrap().stop_duration());
    }

    fn on_vm_started(&mut self, alloc: Allocation) {
        log_debug!(self.ctx, "vm #{} started and running", alloc.id);
        let vm = self.vms.get(&alloc.id).unwrap();
        self.ctx.emit_self(AllocationReleaseRequest { alloc }, vm.lifetime());
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
        log_debug!(self.ctx, "host #{} sends it`s data to monitoring", self.id);
        let added = self.previously_added_vms.to_vec();
        let removed = self.previously_removed_vms.to_vec();
        self.ctx.emit(
            HostStateUpdate {
                host_id: self.id,
                cpu_load: self.get_cpu_load(self.ctx.time()),
                memory_load: self.get_memory_load(self.ctx.time()),
                previously_added_vms: added,
                previously_removed_vms: removed,
            },
            self.monitoring_id,
            self.sim_config.message_delay,
        );

        self.previously_added_vms.clear();
        self.previously_removed_vms.clear();
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
