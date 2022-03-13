use std::collections::HashMap;

use log::info;

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;

use crate::common::AllocationVerdict;
use crate::energy_manager::EnergyManager;
use crate::events::allocation::{AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest};
use crate::events::monitoring::HostStateUpdate;
use crate::events::vm::{VMDeleted, VMStarted};
use crate::network::MESSAGE_DELAY;
use crate::resource_pool::Allocation;
use crate::vm::VirtualMachine;

static STATS_SEND_PERIOD: f64 = 0.5;

pub struct HostManager {
    pub id: String,

    cpu_total: u32,
    cpu_available: u32,

    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,

    cpu_overcommit: u32,
    memory_overcommit: u64,

    allow_vm_overcommit: bool,
    allocs: HashMap<String, Allocation>,
    vms: HashMap<String, VirtualMachine>,
    energy_manager: EnergyManager,
    monitoring_id: String,
    placement_store_id: String,

    ctx: SimulationContext,
}

impl HostManager {
    pub fn new(
        cpu_total: u32,
        memory_total: u64,
        monitoring_id: String,
        placement_store_id: String,
        allow_vm_overcommit: bool,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id().to_string(),
            cpu_total,
            memory_total,
            cpu_available: cpu_total,
            memory_available: memory_total,
            cpu_overcommit: 0,
            memory_overcommit: 0,
            allow_vm_overcommit,
            allocs: HashMap::new(),
            vms: HashMap::new(),
            energy_manager: EnergyManager::new(),
            monitoring_id,
            placement_store_id,
            ctx,
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

        self.allocs.insert(alloc.id.clone(), alloc.clone());
        self.vms.insert(alloc.id.clone(), vm);
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
        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

    fn get_cpu_load(&self, time: f64) -> f64 {
        let mut cpu_used = 0.;
        for (vm_id, alloc) in &self.allocs {
            cpu_used += alloc.cpu_usage as f64 * self.vms[vm_id].get_cpu_load(time);
        }
        return cpu_used / self.cpu_total as f64;
    }

    fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for (vm_id, alloc) in &self.allocs {
            memory_used += alloc.memory_usage as f64 * self.vms[vm_id].get_cpu_load(time);
        }
        return memory_used / self.memory_total as f64;
    }

    fn get_energy_load(&self, time: f64) -> f64 {
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

    fn on_allocation_request(&mut self, alloc: Allocation, vm: VirtualMachine) {
        if self.can_allocate(&alloc) == AllocationVerdict::Success {
            let start_duration = vm.start_duration();
            self.allocate(self.ctx.time(), &alloc, vm);
            info!(
                "[time = {}] vm #{} allocated on host #{}",
                self.ctx.time(),
                alloc.id,
                self.id
            );
            self.ctx.emit_self(VMStarted { alloc }, start_duration);
        } else {
            info!(
                "[time = {}] not enough space for vm #{} on host #{}",
                self.ctx.time(),
                alloc.id,
                self.id
            );
            self.ctx.emit(
                AllocationFailed {
                    alloc,
                    host_id: self.id.clone(),
                },
                &self.placement_store_id,
                MESSAGE_DELAY,
            );
        }
    }

    fn on_allocation_release_request(&mut self, alloc: Allocation) {
        info!(
            "[time = {}] release resources from vm #{} on host #{}",
            self.ctx.time(),
            alloc.id,
            self.id
        );
        let vm = self.vms.get(&alloc.id).unwrap();
        self.ctx.emit_self(VMDeleted { alloc }, vm.stop_duration());
    }

    fn on_vm_started(&mut self, alloc: Allocation) {
        info!("[time = {}] vm #{} started and running", self.ctx.time(), alloc.id);
        let vm = self.vms.get(&alloc.id).unwrap();
        self.ctx.emit_self(AllocationReleaseRequest { alloc }, vm.lifetime());
    }

    fn on_vm_deleted(&mut self, alloc: Allocation) {
        info!("[time = {}] vm #{} deleted", self.ctx.time(), alloc.id);
        self.release(self.ctx.time(), &alloc);
        self.ctx.emit(
            AllocationReleased {
                alloc,
                host_id: self.id.clone(),
            },
            &self.placement_store_id,
            MESSAGE_DELAY,
        );
    }

    fn send_host_state(&mut self) {
        info!(
            "[time = {}] host #{} sends it`s data to monitoring",
            self.ctx.time(),
            self.id
        );
        self.ctx.emit(
            HostStateUpdate {
                host_id: self.id.clone(),
                cpu_load: self.get_cpu_load(self.ctx.time()),
                memory_load: self.get_memory_load(self.ctx.time()),
            },
            &self.monitoring_id,
            MESSAGE_DELAY,
        );

        self.ctx.emit_self(SendHostState {}, STATS_SEND_PERIOD);
    }
}

#[derive(Debug)]
pub struct SendHostState {}

impl EventHandler for HostManager {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { alloc, vm } => {
                self.on_allocation_request(alloc, vm);
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
