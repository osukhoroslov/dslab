use log::info;
use std::collections::HashMap;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

use crate::common::AllocationVerdict;
use crate::energy_manager::EnergyManager;
use crate::events::allocation::{AllocationFailed, AllocationReleaseRequest, AllocationReleased, AllocationRequest};
use crate::events::monitoring::HostStateUpdate;
use crate::events::vm::{VMDeleteRequest, VMDeleted, VMStartRequest, VMStarted};
use crate::network::MESSAGE_DELAY;
use crate::vm::VirtualMachine;

pub static STATS_SEND_PERIOD: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct HostManager {
    pub id: String,

    cpu_total: u32,
    cpu_available: u32,

    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,

    vms: HashMap<String, VirtualMachine>,
    energy_manager: EnergyManager,
    monitoring: ActorId,
    placement_store: ActorId,
}

impl HostManager {
    pub fn new(id: &str, cpu_total: u32, memory_total: u64, monitoring: ActorId, placement_store: ActorId) -> Self {
        Self {
            id: id.to_string(),
            cpu_total,
            memory_total,
            cpu_available: cpu_total,
            memory_available: memory_total,
            vms: HashMap::new(),
            energy_manager: EnergyManager::new(),
            monitoring,
            placement_store,
        }
    }

    fn can_allocate(&self, vm: &VirtualMachine) -> AllocationVerdict {
        if self.cpu_available < vm.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.memory_available < vm.memory_usage {
            return AllocationVerdict::NotEnoughMemory;
        }
        return AllocationVerdict::Success;
    }

    fn place_vm(&mut self, time: f64, vm: &VirtualMachine) {
        self.cpu_available -= vm.cpu_usage;
        self.memory_available -= vm.memory_usage;
        self.vms.insert(vm.id.clone(), vm.clone());

        self.energy_manager.update_energy(time, self.get_energy_load(time));
    }

    fn remove_vm(&mut self, time: f64, vm_id: &str) -> VirtualMachine {
        self.cpu_available += self.vms[vm_id].cpu_usage;
        self.memory_available += self.vms[vm_id].memory_usage;
        let result = self.vms.remove(vm_id).unwrap();
        self.energy_manager.update_energy(time, self.get_energy_load(time));

        result
    }

    fn get_cpu_load(&self, time: f64) -> f64 {
        let mut cpu_used = 0.;
        for (_vm_id, vm) in &self.vms {
            cpu_used += f64::from(vm.cpu_usage) * vm.get_current_cpu_load(time);
        }

        return cpu_used as f64 / (self.cpu_total as f64);
    }

    fn get_memory_load(&self, time: f64) -> f64 {
        let mut memory_used = 0.;
        for (_vm_id, vm) in &self.vms {
            memory_used += vm.memory_usage as f64 * vm.get_current_memory_load(time);
        }

        return memory_used as f64 / (self.memory_total as f64);
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

    fn on_allocation_request(&mut self, vm: &VirtualMachine, ctx: &mut ActorContext) {
        if self.can_allocate(vm) == AllocationVerdict::Success {
            self.place_vm(ctx.time(), vm);
            info!("[time = {}] vm #{} allocated on host #{}", ctx.time(), vm.id, self.id);

            ctx.emit_now(
                VMStartRequest {
                    host_id: self.id.clone(),
                },
                vm.actor_id.clone(),
            );
        } else {
            info!(
                "[time = {}] not enough space for vm #{} on host #{}",
                ctx.time(),
                vm.id,
                self.id
            );
            ctx.emit(
                AllocationFailed {
                    vm: vm.clone(),
                    host_id: self.id.clone(),
                },
                self.placement_store.clone(),
                MESSAGE_DELAY,
            );
        }
    }

    fn on_allocation_release_request(&mut self, vm: &VirtualMachine, ctx: &mut ActorContext) {
        info!(
            "[time = {}] release resources from vm #{} on host #{}",
            ctx.time(),
            vm.id,
            self.id
        );
        ctx.emit_now(VMDeleteRequest {}, vm.actor_id.clone());
    }

    fn on_vm_started(&mut self, vm_id: &String, ctx: &mut ActorContext) {
        info!("[time = {}] vm #{} started and running", ctx.time(), vm_id);
    }

    fn on_vm_deleted(&mut self, vm_id: &String, ctx: &mut ActorContext) {
        info!("[time = {}] vm #{} deleted", ctx.time(), vm_id);
        let vm = self.remove_vm(ctx.time(), &vm_id);
        ctx.emit(
            AllocationReleased {
                vm,
                host_id: self.id.clone(),
            },
            self.placement_store.clone(),
            MESSAGE_DELAY,
        );
    }

    fn send_host_state(&self, ctx: &mut ActorContext) {
        info!(
            "[time = {}] host #{} sends it`s data to monitoring",
            ctx.time(),
            self.id
        );
        ctx.emit(
            HostStateUpdate {
                host_id: ctx.id.to_string(),
                cpu_load: self.get_cpu_load(ctx.time()),
                memory_load: self.get_memory_load(ctx.time()),
            },
            self.monitoring.clone(),
            MESSAGE_DELAY,
        );

        ctx.emit_self(SendHostState {}, STATS_SEND_PERIOD);
    }
}

#[derive(Debug)]
pub struct SendHostState {}

impl Actor for HostManager {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            AllocationRequest { vm } => {
                self.on_allocation_request(vm, ctx);
            }
            AllocationReleaseRequest { vm } => {
                self.on_allocation_release_request(vm, ctx);
            }
            VMStarted { vm_id } => {
                self.on_vm_started(vm_id, ctx);
            }
            VMDeleted { vm_id } => {
                self.on_vm_deleted(vm_id, ctx);
            }
            SendHostState {} => {
                self.send_host_state(ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
