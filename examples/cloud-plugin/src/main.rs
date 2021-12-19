mod allocation_agent;
mod host;
mod monitoring;
mod virtual_machine;

use std::rc::Rc;
use std::cell::RefCell;
use sugars::{rc, refcell};

use core::sim::Simulation;
use core::actor::{ActorId};

use allocation_agent::AllocationAgent;
use monitoring::Monitoring;

use crate::allocation_agent::FindHostToAllocateVM;
use crate::host::SendMonitoringStats;
use crate::host::HostAllocationAgent;
use crate::virtual_machine::VirtualMachine;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    simulation: Simulation
}

impl CloudSimulation {
    pub fn new(sim: Simulation) -> Self {
        Self {
            monitoring: rc!(refcell!(Monitoring::new(ActorId::from("monitoring")))),
            simulation: sim,
        }
    }

    pub fn init_actors(&mut self) {
        let _actor = self.simulation.add_actor(&"monitoring".to_string(), self.monitoring.clone());
    }

    pub fn spawn_host(&mut self, id: String, cpu_capacity: u32, ram_capacity: u32) -> ActorId {
        let host = rc!(refcell!(HostAllocationAgent::new(
                cpu_capacity, ram_capacity, id.clone(), ActorId::from("monitoring"))));

        let actor = self.simulation.add_actor(&id.clone(), host.clone());
        self.simulation.add_event(SendMonitoringStats { }, actor.clone(), actor.clone(), 0.); 
        return actor;
    }

    pub fn spawn_allocator(&mut self, id: String) -> ActorId {
        let allocator = rc!(refcell!(AllocationAgent::new(ActorId::from(&id.clone()), 
                                                          self.monitoring.clone())));
        let actor = self.simulation.add_actor(&id.clone(), allocator.clone());
        return actor;
    }

    pub fn spawn_vm(&mut self, id: String,
                    cpu_usage: u32,
                    ram_usage: u32,
                    lifetime: f64,
                    allocator: ActorId) -> ActorId {
        let vm = VirtualMachine::new(id.clone(), cpu_usage, ram_usage, lifetime);
        let actor = self.simulation.add_actor(&id.clone(), rc!(refcell!(vm.clone())).clone());
        self.simulation.add_event(FindHostToAllocateVM { vm },
                                  allocator.clone(),
                                  allocator.clone(), 0.0);
        return actor;
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        return self.simulation.steps(step_count);
    }
}

fn main() {
    let sim = Simulation::new(123);
    let mut cloud_sim = CloudSimulation::new(sim);
    cloud_sim.init_actors();

    let _host_one = cloud_sim.spawn_host("h1".to_string(), 30, 30);
    let _host_two = cloud_sim.spawn_host("h2".to_string(), 30, 30);
    let allocator = cloud_sim.spawn_allocator("a".to_string());

    for i in 0..10 {
        let vm_name = "v".to_owned() + &i.to_string();
        let _vm = cloud_sim.spawn_vm(vm_name.clone(), 10, 10, 2.0, allocator.clone());
    }

    cloud_sim.steps(250);
}
