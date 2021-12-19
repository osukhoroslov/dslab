use core::match_event;
use core::actor::{ActorId, ActorContext, Event, Actor};

use crate::allocation_agent::FindHostToAllocateVM;
use crate::monitoring::UpdateHostStats;
use crate::virtual_machine::VMStart;
use crate::virtual_machine::VirtualMachine;

#[derive(PartialEq)]
enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughRAM,
    Success,
}

pub static VM_INIT_TIME: f64 = 1.0;
pub static VM_FINISH_TIME: f64 = 0.5;
pub static STATS_SEND_PERIOD: f64 = 0.5;
pub static MESSAGE_DELAY: f64 = 0.2;
pub static ALLOCATION_RETRY_PERIOD: f64 = 1.0;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EnergyManager {
    energy_consumed: f64,
    prev_milestone: f64,
    current_load: f64
}

#[derive(Debug, Clone)]
pub struct HostAllocationAgent {
    pub id: String,

    cpu_total: u32,
    cpu_available: u32,
    ram_total: u32,
    ram_available: u32,

    vms: Vec<VirtualMachine>,
    vm_counter: u64,

    energy_manager: EnergyManager,

    monitoring: ActorId
}

impl EnergyManager {
    pub fn new() -> Self {
        Self {
            prev_milestone: 0.0,
            energy_consumed: 0.0,
            current_load: 0.0
        }
    }

    pub fn update_energy(&mut self, time: f64, new_load: f64) {
        self.energy_consumed += (time - self.prev_milestone) * self.current_load;
        self.current_load = new_load;
    }

    pub fn get_total_consumed(self) -> f64 {
        return self.energy_consumed;
    }
}

impl HostAllocationAgent {
    pub fn new(cpu_total: u32, ram_total: u32, id: String, monitoring: ActorId) -> Self {
        Self {
            id: id,
            cpu_total: cpu_total,
            ram_total: ram_total,
            cpu_available: cpu_total,
            ram_available: ram_total,
            vms: Vec::new(),
            vm_counter: 0,
            energy_manager: EnergyManager::new(),
            monitoring: monitoring.clone()
        }
    }

    fn can_allocate(&self, vm: VirtualMachine) -> AllocationVerdict {
        if self.cpu_available < vm.cpu_usage {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.ram_available < vm.ram_usage {
            return AllocationVerdict::NotEnoughRAM;
        }
        return AllocationVerdict::Success;
    }

    fn get_energy_load(&self) -> f64 {
        let cpu_used = (self.cpu_total - self.cpu_available) as f64;
        return 0.4 + 0.6 * cpu_used / (self.cpu_total as f64);
    }

    fn pickup_vm(&mut self, time: f64, vm: VirtualMachine) {
        self.cpu_available -= vm.cpu_usage;
        self.ram_available -= vm.ram_usage;

        self.energy_manager.update_energy(time, self.get_energy_load());
        self.vms.push(vm.clone())
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TryAllocateVM {
    pub vm: VirtualMachine,
}

#[derive(Debug)]
pub struct SendMonitoringStats {
}

#[derive(Debug)]
pub struct ReleaseVmResourses {
    pub vm_id: String
}

impl Actor for HostAllocationAgent {
    fn on(&mut self, event: Box<dyn Event>, 
                     from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            TryAllocateVM { vm } => {
                if self.can_allocate(vm.clone()) == AllocationVerdict::Success {
                    self.pickup_vm(ctx.time(), vm.clone());
                    println!("[time = {}] vm #{} allocated on host #{}",
                         ctx.time(), vm.id, self.id);
   
                    ctx.emit(VMStart { }, vm.actor_id.clone(), VM_INIT_TIME);
                } else {
                    println!("[time = {}] not enouth space for vm #{} on host #{}",
                        ctx.time(), vm.id, self.id);
                    ctx.emit(FindHostToAllocateVM { vm: vm.clone() }, from, MESSAGE_DELAY);
                }
            },
            SendMonitoringStats { } => {
                println!("[time = {}] host #{} sends it`s data to monitoring", ctx.time(), self.id);
                ctx.emit(UpdateHostStats {
                        host_id: ctx.id.clone(),
                        cpu_available: self.cpu_available,
                        ram_available: self.ram_available
                    },
                    self.monitoring.clone(), MESSAGE_DELAY
                );

                ctx.emit(SendMonitoringStats { }, ctx.id.clone(), STATS_SEND_PERIOD);
            },
            ReleaseVmResourses { vm_id } => {
                println!("[time = {}] release resourses from vm #{} in host #{}",
                    ctx.time(), vm_id, self.id
                );
                for i in 0..self.vms.len() {
                    if self.vms[i].id == *vm_id {
                        self.cpu_available += self.vms[i].cpu_usage;
                        self.ram_available += self.vms[i].ram_usage;

                        let _deleted = self.vms.swap_remove(i);
                        break;
                    }
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
