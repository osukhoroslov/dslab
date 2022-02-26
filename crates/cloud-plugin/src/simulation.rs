use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sugars::{rc, refcell};

use core::context::SimulationContext;
use core::simulation::Simulation;

use crate::events::allocation::AllocationRequest;
use crate::host_manager::HostManager;
use crate::host_manager::SendHostState;
use crate::monitoring::Monitoring;
use crate::placement_store::PlacementStore;
use crate::resource_pool::Allocation;
use crate::scheduler::Scheduler;
use crate::vm::VirtualMachine;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    monitoring_id: String,
    placement_store: Rc<RefCell<PlacementStore>>,
    placement_store_id: String,
    hosts: BTreeMap<String, Rc<RefCell<HostManager>>>,
    schedulers: BTreeMap<String, Rc<RefCell<Scheduler>>>,
    sim: Simulation,
    ctx: SimulationContext,
}

impl CloudSimulation {
    pub fn new(mut sim: Simulation) -> Self {
        let monitoring_id = "monitoring";
        let monitoring = rc!(refcell!(Monitoring::new()));
        sim.add_handler(monitoring_id, monitoring.clone());
        let placement_store_id = "placement_store";
        let placement_store = rc!(refcell!(PlacementStore::new(sim.create_context(placement_store_id))));
        sim.add_handler(placement_store_id, placement_store.clone());
        let ctx = sim.create_context("simulation");
        Self {
            monitoring,
            monitoring_id: monitoring_id.to_string(),
            placement_store,
            placement_store_id: placement_store_id.to_string(),
            hosts: BTreeMap::new(),
            schedulers: BTreeMap::new(),
            sim,
            ctx,
        }
    }

    pub fn add_host(&mut self, id: &str, cpu_total: u32, memory_total: u64) {
        // create host
        let host = rc!(refcell!(HostManager::new(
            cpu_total,
            memory_total,
            self.monitoring_id.clone(),
            self.placement_store_id.clone(),
            self.sim.create_context(id),
        )));
        self.sim.add_handler(id, host.clone());
        self.hosts.insert(id.to_string(), host);
        // add host to monitoring
        self.monitoring.borrow_mut().add_host(id, cpu_total, memory_total);
        // add host to placement store
        self.placement_store.borrow_mut().add_host(id, cpu_total, memory_total);
        // add host to schedulers
        for (_, scheduler) in &self.schedulers {
            scheduler.borrow_mut().add_host(id, cpu_total, memory_total);
        }
        // start sending host state to monitoring
        self.ctx.emit_now(SendHostState {}, id);
    }

    pub fn add_scheduler(&mut self, id: &str) {
        // create scheduler using current state from placement store
        let pool_state = self.placement_store.borrow_mut().get_pool_state();
        let scheduler = rc!(refcell!(Scheduler::new(
            pool_state,
            self.monitoring.clone(),
            self.placement_store_id.clone(),
            self.sim.create_context(id),
        )));
        self.sim.add_handler(id, scheduler.clone());
        self.schedulers.insert(id.to_string(), scheduler);
        // notify placement store
        self.placement_store.borrow_mut().add_scheduler(id);
    }

    pub fn spawn_vm(&mut self, id: &str, cpu_usage: u32, memory_usage: u64, lifetime: f64, scheduler: &str) {
        self.ctx.emit_now(
            AllocationRequest {
                alloc: Allocation {
                    id: id.to_string(),
                    cpu_usage,
                    memory_usage,
                },
                vm: VirtualMachine::new(lifetime),
            },
            scheduler,
        );
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    pub fn current_time(&mut self) -> f64 {
        return self.sim.time();
    }

    pub fn host(&mut self, host_id: &str) -> Rc<RefCell<HostManager>> {
        self.hosts.get(host_id).unwrap().clone()
    }
}
