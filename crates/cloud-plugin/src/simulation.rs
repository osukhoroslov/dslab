use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use sugars::{rc, refcell};

use simcore::context::SimulationContext;
use simcore::simulation::Simulation;

use crate::config::SimulationConfig;
use crate::events::allocation::{AllocationRequest, MigrationRequest};
use crate::host_manager::HostManager;
use crate::host_manager::SendHostState;
use crate::load_model::LoadModel;
use crate::monitoring::Monitoring;
use crate::placement_store::PlacementStore;
use crate::resource_pool::Allocation;
use crate::scheduler::Scheduler;
use crate::vm::VirtualMachine;
use crate::vm_placement_algorithm::VMPlacementAlgorithm;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    monitoring_id: u32,
    placement_store: Rc<RefCell<PlacementStore>>,
    placement_store_id: u32,
    hosts: BTreeMap<u32, Rc<RefCell<HostManager>>>,
    vms: BTreeMap<u32, VirtualMachine>,
    allocations: HashMap<u32, Allocation>,
    schedulers: HashMap<u32, Rc<RefCell<Scheduler>>>,
    sim: Simulation,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl CloudSimulation {
    pub fn new(mut sim: Simulation, sim_config: SimulationConfig) -> Self {
        let monitoring_id = "monitoring";
        let monitoring = rc!(refcell!(Monitoring::new(sim.create_context("monitoring"))));
        let monitoring_id_as_u32 = sim.add_handler(monitoring_id, monitoring.clone());
        let placement_store_id = "placement_store";
        let placement_store = rc!(refcell!(PlacementStore::new(
            sim_config.allow_vm_overcommit,
            sim.create_context(placement_store_id),
            sim_config.clone(),
        )));
        let placement_store_id_as_u32 = sim.add_handler(placement_store_id, placement_store.clone());
        let ctx = sim.create_context("simulation");
        Self {
            monitoring,
            monitoring_id: monitoring_id_as_u32,
            placement_store,
            placement_store_id: placement_store_id_as_u32,
            hosts: BTreeMap::new(),
            vms: BTreeMap::new(),
            allocations: HashMap::new(),
            schedulers: HashMap::new(),
            sim,
            ctx,
            sim_config: rc!(sim_config),
        }
    }

    pub fn add_host(&mut self, name: &str, cpu_total: u32, memory_total: u64) -> u32 {
        // create host
        let host = rc!(refcell!(HostManager::new(
            cpu_total,
            memory_total,
            self.monitoring_id,
            self.placement_store_id,
            self.sim_config.allow_vm_overcommit,
            self.sim.create_context(name),
            self.sim_config.clone(),
        )));
        let id = self.sim.add_handler(name, host.clone());
        self.hosts.insert(id, host);
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
        id
    }

    pub fn add_scheduler(&mut self, name: &str, vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>) -> u32 {
        // create scheduler using current state from placement store
        let pool_state = self.placement_store.borrow_mut().get_pool_state();
        let scheduler = rc!(refcell!(Scheduler::new(
            pool_state,
            self.monitoring.clone(),
            self.placement_store_id,
            vm_placement_algorithm,
            self.sim.create_context(name),
            self.sim_config.clone(),
        )));
        let id = self.sim.add_handler(name, scheduler.clone());
        self.schedulers.insert(id, scheduler);
        // notify placement store
        self.placement_store.borrow_mut().add_scheduler(id);
        id
    }

    pub fn spawn_vm_now(
        &mut self,
        id: u32,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        scheduler: u32,
    ) {
        let alloc = Allocation {
            id: id,
            cpu_usage,
            memory_usage,
        };
        let vm = VirtualMachine::new(lifetime, cpu_load_model, memory_load_model, self.sim_config.clone());

        self.vms.insert(id, vm.clone());
        self.allocations.insert(id, alloc.clone());

        self.ctx.emit_now(
            AllocationRequest {
                alloc: alloc.clone(),
                vm: vm.clone(),
            },
            scheduler,
        );
    }

    pub fn spawn_vm_with_delay(
        &mut self,
        id: u32,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        scheduler: u32,
        delay: f64,
    ) {
        self.ctx.emit(
            AllocationRequest {
                alloc: Allocation {
                    id: id,
                    cpu_usage,
                    memory_usage,
                },
                vm: VirtualMachine::new(lifetime, cpu_load_model, memory_load_model, self.sim_config.clone()),
            },
            scheduler,
            delay,
        );
    }

    pub fn migrate_vm_to_host(&mut self, vm_id: u32, destination_host_id: u32) {
        let alloc = self.allocations.get(&vm_id).unwrap();
        let mut vm = self.vms.get_mut(&vm_id).unwrap();
        vm.lifetime -= self.ctx.time() - vm.start_time;
        let source_host = self.monitoring.borrow_mut().find_host_by_vm(vm_id);

        self.ctx.emit(
            MigrationRequest {
                source_host: source_host.clone(),
                alloc: alloc.clone(),
                vm: vm.clone(),
            },
            destination_host_id,
            self.sim_config.message_delay,
        );
    }

    pub fn context(&self) -> &SimulationContext {
        return &self.ctx;
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    pub fn event_count(&self) -> u64 {
        return self.sim.event_count();
    }

    pub fn current_time(&mut self) -> f64 {
        return self.sim.time();
    }

    pub fn sleep_for(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    pub fn host(&self, host_id: u32) -> Rc<RefCell<HostManager>> {
        self.hosts.get(&host_id).unwrap().clone()
    }
}
