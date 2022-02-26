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
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id().to_string(),
            cpu_total,
            memory_total,
            cpu_available: cpu_total,
            memory_available: memory_total,
            vms: HashMap::new(),
            energy_manager: EnergyManager::new(),
            monitoring_id,
            placement_store_id,
            ctx,
        }
    }

    fn can_allocate(&self, alloc: &Allocation) -> AllocationVerdict {
        if self.cpu_available < alloc.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.memory_available < alloc.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    fn allocate(&mut self, time: f64, alloc: &Allocation, vm: VirtualMachine) {
        self.cpu_available -= alloc.cpu_usage;
        self.memory_available -= alloc.memory_usage;
        self.vms.insert(alloc.id.clone(), vm);
        self.energy_manager.update_energy(time, self.get_energy_load());
    }

    fn release(&mut self, time: f64, alloc: &Allocation) {
        self.cpu_available += alloc.cpu_usage;
        self.memory_available += alloc.memory_usage;
        self.energy_manager.update_energy(time, self.get_energy_load());
        self.vms.remove(&alloc.id);
    }

    fn get_energy_load(&self) -> f64 {
        let cpu_used = (self.cpu_total - self.cpu_available) as f64;
        if cpu_used == 0. {
            return 0.;
        }
        return 0.4 + 0.6 * cpu_used / (self.cpu_total as f64);
    }

    pub fn get_total_consumed(&mut self, time: f64) -> f64 {
        self.energy_manager.update_energy(time, self.get_energy_load());
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
                cpu_available: self.cpu_available,
                memory_available: self.memory_available,
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
