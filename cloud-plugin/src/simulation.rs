use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use sugars::{rc, refcell};

use core::actor::ActorId;
use core::sim::Simulation;

use crate::events::allocation::AllocationRequest;
use crate::host_manager::HostManager;
use crate::host_manager::SendHostState;
use crate::monitoring::Monitoring;
use crate::placement_store::PlacementStore;
use crate::scheduler::Scheduler;
use crate::vm::VirtualMachine;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    monitoring_id: ActorId,
    placement_store: Rc<RefCell<PlacementStore>>,
    placement_store_id: ActorId,
    hosts: BTreeMap<String, Rc<RefCell<HostManager>>>,
    schedulers: BTreeMap<String, Rc<RefCell<Scheduler>>>,
    simulation: Simulation,
    simulation_id: ActorId,
}

impl CloudSimulation {
    pub fn new(mut sim: Simulation) -> Self {
        let monitoring = rc!(refcell!(Monitoring::new()));
        let monitoring_id = sim.add_actor("monitoring", monitoring.clone());
        let placement_store = rc!(refcell!(PlacementStore::new()));
        let placement_store_id = sim.add_actor("placement_store", placement_store.clone());
        Self {
            monitoring,
            monitoring_id,
            placement_store,
            placement_store_id,
            hosts: BTreeMap::new(),
            schedulers: BTreeMap::new(),
            simulation: sim,
            simulation_id: ActorId::from("simulation"),
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        // create host actor
        let host = rc!(refcell!(HostManager::new(
            id,
            cpu_total,
            memory_total,
            self.monitoring_id.clone(),
            self.placement_store_id.clone()
        )));
        let actor_id = self.simulation.add_actor(id, host.clone());
        self.hosts.insert(id.to_string(), host);

        // add host to monitoring
        self.monitoring
            .borrow_mut()
            .add_host(id.to_string(), cpu_total, memory_total);

        // add host to placement store
        self.placement_store.borrow_mut().add_host(id, cpu_total, memory_total);

        // add host to schedulers
        for (_, sched) in &self.schedulers {
            sched.borrow_mut().add_host(id, cpu_total, memory_total);
        }

        // start sending host state to monitoring
        self.simulation
            .add_event_now(SendHostState {}, self.simulation_id.clone(), actor_id.clone());
    }

    pub fn add_scheduler(&mut self, id: &str) {
        // create scheduler actor using current state from placement store
        let pool_state = self.placement_store.borrow_mut().get_pool_state();
        let scheduler = rc!(refcell!(Scheduler::new(
            ActorId::from(id),
            pool_state,
            self.monitoring.clone(),
            self.placement_store_id.clone()
        )));
        self.simulation.add_actor(id, scheduler.clone());
        self.schedulers.insert(id.to_string(), scheduler);

        // notify placement store
        self.placement_store.borrow_mut().add_scheduler(id);
    }

    pub fn spawn_vm(&mut self, id: &str, cpu_usage: u32, memory_usage: u64, lifetime: f64, scheduler: &str) -> ActorId {
        let vm = VirtualMachine::new(id, cpu_usage, memory_usage, lifetime);
        let actor = self.simulation.add_actor(id, rc!(refcell!(vm.clone())));
        self.simulation.add_event_now(
            AllocationRequest { vm },
            self.simulation_id.clone(),
            ActorId::from(scheduler),
        );
        return actor;
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        return self.simulation.steps(step_count);
    }

    pub fn current_time(&mut self) -> f64 {
        return self.simulation.time();
    }

    pub fn host(&mut self, host_id: &str) -> Rc<RefCell<HostManager>> {
        self.hosts.get(host_id).unwrap().clone()
    }
}
