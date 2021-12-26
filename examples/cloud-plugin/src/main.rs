mod scheduler;
mod host;
mod monitoring;
mod network;
mod virtual_machine;

use std::rc::Rc;
use std::cell::RefCell;
use sugars::{rc, refcell};

use core::sim::Simulation;
use core::actor::ActorId;

use scheduler::Scheduler;
use monitoring::Monitoring;

use crate::scheduler::FindHostToAllocateVM;
use crate::host::SendHostState;
use crate::host::HostManager;
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
        self.simulation.add_actor("monitoring", self.monitoring.clone());
    }

    pub fn spawn_host(&mut self, id: &str, cpu_capacity: u32, ram_capacity: u32) -> ActorId {
        let host = rc!(refcell!(HostManager::new(
                cpu_capacity, ram_capacity, id.to_string(), ActorId::from("monitoring"))));

        let actor = self.simulation.add_actor(&id.clone(), host);
        self.simulation.add_event(SendHostState { }, actor.clone(), actor.clone(), 0.); 
        return actor;
    }

    pub fn spawn_allocator(&mut self, id: &str) -> ActorId {
        let allocator = rc!(refcell!(Scheduler::new(ActorId::from(&id.clone()), 
                                                          self.monitoring.clone())));
        self.simulation.add_actor(&id.clone(), allocator)
    }

    pub fn spawn_vm(&mut self, id: &str,
                    cpu_usage: u32,
                    ram_usage: u32,
                    lifetime: f64,
                    allocator: ActorId) -> ActorId {
        let vm = VirtualMachine::new(id, cpu_usage, ram_usage, lifetime);
        let actor = self.simulation.add_actor(id, rc!(refcell!(vm.clone())));
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

    let _host_one = cloud_sim.spawn_host("h1", 30, 30);
    let _host_two = cloud_sim.spawn_host("h2", 30, 30);
    let allocator = cloud_sim.spawn_allocator("a");

    for i in 0..10 {
        let vm_name = format!("v{}", i);
        let _vm = cloud_sim.spawn_vm(&vm_name, 10, 10, 2.0, allocator.clone());
    }

    cloud_sim.steps(250);
}
