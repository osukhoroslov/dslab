use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::placement_store::OnNewHostAdded;
use crate::placement_store::PlacementStore;
use crate::scheduler::FindHostToAllocateVM;
use crate::scheduler::Scheduler;
use crate::virtual_machine::VirtualMachine;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    simulation: Simulation,
}

impl CloudSimulation {
    pub fn new(sim: Simulation) -> Self {
        let monitoring = rc!(refcell!(Monitoring::new()));
        Self {
            monitoring: rc!(refcell!(Monitoring::new())),
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

        let actor = self.simulation.add_actor(id, host.clone());
        self.simulation
            .add_event_now(SendHostState {}, actor.clone(), actor.clone());
        self.simulation
            .add_event_now(OnNewHostAdded { id: id.to_string(),
                host: HostState::new(ActorId::from(&id), cpu_capacity, memory_capacity)}, 
                ActorId::from("placement_store"), ActorId::from("placement_store"));
        return host;
    }

    pub fn spawn_allocator(&mut self, id: &str) -> ActorId {
        let allocator = rc!(refcell!(Scheduler::new(ActorId::from(id), self.monitoring.clone())));
        self.simulation.add_actor(id, allocator)
    }

    pub fn spawn_vm(&mut self, id: &str, cpu_usage: u32, ram_usage: u32, lifetime: f64, allocator: ActorId) -> ActorId {
        let vm = VirtualMachine::new(id, cpu_usage, ram_usage, lifetime);
        let actor = self.simulation.add_actor(id, rc!(refcell!(vm.clone())));
        self.simulation
            .add_event(FindHostToAllocateVM { vm }, allocator.clone(), allocator.clone(), 0.0);
        return actor;
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        return self.simulation.steps(step_count);
    }

    pub fn current_time(&mut self) -> f64 {
        return self.simulation.time();
    }
}
