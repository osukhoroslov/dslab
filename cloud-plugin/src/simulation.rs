use std::cell::RefCell;
use std::rc::Rc;
use sugars::{rc, refcell};

use core::actor::ActorId;
use core::sim::Simulation;

use crate::host::HostManager;
use crate::host::SendHostState;
use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::placement_store::OnNewHostAdded;
use crate::placement_store::PlacementStore;
use crate::scheduler::FindHostToAllocateVM;
use crate::scheduler::Scheduler;
use crate::virtual_machine::VirtualMachine;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    placement_store: Rc<RefCell<PlacementStore>>,
    simulation: Simulation,
}

impl CloudSimulation {
    pub fn new(sim: Simulation) -> Self {
        let monitoring = rc!(refcell!(Monitoring::new()));
        Self {
            monitoring: monitoring.clone(),
            placement_store: rc!(refcell!(PlacementStore::new(monitoring.clone()))),
            simulation: sim,
        }
    }

    pub fn init_actors(&mut self) {
        self.simulation.add_actor("placement_store", self.placement_store.clone());
        self.simulation.add_actor("monitoring", self.monitoring.clone());
    }

    pub fn spawn_host(&mut self, id: &str, cpu_capacity: u32, memory_capacity: u32) -> Rc<RefCell<HostManager>> {
        let host = rc!(refcell!(HostManager::new(
            cpu_capacity,
            memory_capacity,
            id.to_string(),
            ActorId::from("monitoring")
        )));
        self.monitoring.borrow_mut().add_host(id.to_string(), cpu_capacity, memory_capacity);

        let actor = self.simulation.add_actor(id, host.clone());
        self.simulation
            .add_event_now(SendHostState {}, actor.clone(), actor.clone());
        self.simulation
            .add_event_now(OnNewHostAdded { id: id.to_string(),
                host: HostState::new(ActorId::from(&id), cpu_capacity, memory_capacity)}, 
                ActorId::from("placement_store"), ActorId::from("placement_store"));
        return host;
    }

    pub fn spawn_scheduler(&mut self, id: &str) -> ActorId {
        let scheduler = rc!(refcell!(Scheduler::new(ActorId::from(id),
                                                    self.monitoring.clone(),
                                                    ActorId::from("placement_store"))));
        self.monitoring.borrow_mut().add_scheduler(id.to_string());
        self.simulation.add_actor(id, scheduler)
    }

    pub fn spawn_vm(&mut self, id: &str, cpu_usage: u32, memory_usage: u32, lifetime: f64, scheduler: ActorId) -> ActorId {
        let vm = VirtualMachine::new(id, cpu_usage, memory_usage, lifetime);
        let actor = self.simulation.add_actor(id, rc!(refcell!(vm.clone())));
        self.simulation
            .add_event_now(FindHostToAllocateVM { vm }, scheduler.clone(), scheduler.clone());
        return actor;
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        return self.simulation.steps(step_count);
    }

    pub fn current_time(&mut self) -> f64 {
        return self.simulation.time();
    }
}
