use core::match_event;
use core::actor::{ActorId, Actor, ActorContext, Event};

use crate::virtual_machine::VirtualMachine;
use crate::host::STATS_SEND_LAG;
use crate::host::ALLOCATION_RETRY_PERIOD;
use crate::host::TryAllocateVM;

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CloudBalancer {
    pub id: ActorId,
    hosts: Vec<ActorId>,
    host_cpu_available: Vec<i64>,
    host_ram_available: Vec<i64>,
}

impl CloudBalancer {
    pub fn new(id: ActorId) -> Self {
        Self {
            id: id,
            hosts: Vec::new(),
            host_cpu_available: Vec::new(),
            host_ram_available: Vec::new()
        }
    }

    pub fn add_host(&mut self, host: ActorId) {
        self.hosts.push(host.clone());
        self.host_cpu_available.push(0);
        self.host_ram_available.push(0);
    }

}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FindHostToAllocateVM {
    pub vm: VirtualMachine
}

#[derive(Debug, Clone)]
pub struct UpdateHostStats {
    pub host_id: ActorId,
    pub cpu_available: i64,
    pub ram_available: i64,
}

impl Actor for CloudBalancer {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            FindHostToAllocateVM { vm } => {
                // pack via First Fit policy
                let mut found = false;
                for i in 0..self.hosts.len() {
                    if self.host_cpu_available[i] >= vm.cpu_usage &&
                        self.host_ram_available[i] >= vm.ram_usage {
                        println!("[time = {}] balancer #{} decided to pack vm #{} on host #{}",
                            ctx.time(), self.id, vm.id, self.hosts[i]
                        );
                        found = true;
                        ctx.emit(TryAllocateVM { vm: vm.clone() },
                            self.hosts[i].clone(),
                            STATS_SEND_LAG
                        );
                        break;
                    }
                }
                if (!found) {
                    println!("[time = {}] balancer #{} failed to pack vm #{}",
                        ctx.time(), self.id, vm.id);

                    ctx.emit(FindHostToAllocateVM { vm: vm.clone() },
                            ctx.id.clone(),
                            ALLOCATION_RETRY_PERIOD
                    );
                }
            },
            UpdateHostStats { host_id, cpu_available, ram_available } => {
                println!("[time = {}] balancer #{} received stats from host #{}",
                    ctx.time(), self.id, host_id
                );
                for i in 0..self.hosts.len() {
                    if *host_id == self.hosts[i] {
                        self.host_cpu_available[i] = *cpu_available;
                        self.host_ram_available[i] = *ram_available;
                        break;
                    }
                }
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
