use core::match_event;
use core::actor::{ActorId, ActorContext, Event, Actor};

use crate::virtual_machine::VirtualMachine;
use crate::cloud_balancer::UpdateHostStats;
use crate::cloud_balancer::FindHostToAllocateVM;
use crate::virtual_machine::VMStart;
use core::sim::Simulation;

#[derive(PartialEq)]
enum AllocationVerdict {
    NotEnoughCPU,
    NotEnoughRAM,
    Success,
}

pub static VM_INIT_TIME: f64 = 1.0;
pub static VM_FINISH_TIME: f64 = 0.5;
pub static STATS_SEND_PERIOD: f64 = 0.5;
pub static STATS_SEND_LAG: f64 = 0.2;
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

    cpu_full: i64,
    cpu_available: i64,
    ram_full: i64,
    ram_available: i64,

    vm_s: Vec<VirtualMachine>,
    vm_counter: u64,

    energy_manager: EnergyManager,

    balancers: Vec<ActorId>
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
    pub fn new(cpu_full: i64, ram_full: i64, id: String) -> Self {
        Self {
            id: id,
            cpu_full: cpu_full,
            ram_full: ram_full,
            cpu_available: cpu_full,
            ram_available: ram_full,
            vm_s: Vec::new(),
            vm_counter: 0,
            energy_manager: EnergyManager::new(),
            balancers: Vec::new()
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
        let cpu_used = (self.cpu_full - self.cpu_available) as f64;
        return 0.4 + 0.6 * cpu_used / (self.cpu_full as f64);
    }

    fn pickup_vm(&mut self, time: f64, vm: VirtualMachine) {
        self.cpu_available -= vm.cpu_usage;
        self.ram_available -= vm.ram_usage;

        self.energy_manager.update_energy(time, self.get_energy_load());
        self.vm_s.push(vm.clone())
    }

    pub fn add_subscriber(&mut self, balancer: ActorId) {
        self.balancers.push(balancer.clone())
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
                    ctx.emit(FindHostToAllocateVM { vm: vm.clone() }, from, STATS_SEND_LAG);
                }
            },
            SendMonitoringStats { } => {
                println!("[time = {}] host #{} sends it`s data to balancers", ctx.time(), self.id);
                for balancer in &self.balancers {
                    ctx.emit(UpdateHostStats { 
                            host_id: ctx.id.clone(),
                            cpu_available: self.cpu_available,
                            ram_available: self.ram_available
                        },
                        balancer.clone(), STATS_SEND_LAG
                    );
                }

                ctx.emit(SendMonitoringStats { }, ctx.id.clone(), STATS_SEND_PERIOD);
            },
            ReleaseVmResourses { vm_id } => {
                println!("[time = {}] release resourses from vm #{} in host #{}",
                    ctx.time(), vm_id, self.id
                );
                for i in 0..self.vm_s.len() {
                    if self.vm_s[i].id == *vm_id {
                        self.cpu_available += self.vm_s[i].cpu_usage;
                        self.ram_available += self.vm_s[i].ram_usage;

                        let _deleted = self.vm_s.swap_remove(i);
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
