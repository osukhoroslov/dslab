use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::simulation::Simulation;

use crate::core::common::Allocation;
use crate::core::config::SimulationConfig;
use crate::core::events::allocation::{AllocationRequest, MigrationRequest};
use crate::core::host_manager::HostManager;
use crate::core::host_manager::SendHostState;
use crate::core::load_model::ConstLoadModel;
use crate::core::load_model::LoadModel;
use crate::core::monitoring::Monitoring;
use crate::core::placement_store::PlacementStore;
use crate::core::scheduler::Scheduler;
use crate::core::vm::{VirtualMachine, VmStatus};
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;
use crate::custom_component::CustomComponent;
use crate::extensions::dataset_reader::DatasetReader;

pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    monitoring_id: u32,
    placement_store: Rc<RefCell<PlacementStore>>,
    placement_store_id: u32,
    hosts: BTreeMap<u32, Rc<RefCell<HostManager>>>,
    schedulers: HashMap<u32, Rc<RefCell<Scheduler>>>,
    components: HashMap<u32, Rc<RefCell<dyn CustomComponent>>>,
    sim: Simulation,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl CloudSimulation {
    pub fn new(mut sim: Simulation, sim_config: SimulationConfig) -> Self {
        let monitoring = rc!(refcell!(Monitoring::new(sim.create_context("monitoring"))));
        let monitoring_id = sim.add_handler("monitoring", monitoring.clone());
        let placement_store = rc!(refcell!(PlacementStore::new(
            sim_config.allow_vm_overcommit,
            sim.create_context("placement_store"),
            sim_config.clone(),
        )));
        let placement_store_id = sim.add_handler("placement_store", placement_store.clone());
        let ctx = sim.create_context("simulation");
        Self {
            monitoring,
            monitoring_id,
            placement_store,
            placement_store_id,
            hosts: BTreeMap::new(),
            schedulers: HashMap::new(),
            components: HashMap::new(),
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
        scheduler_id: u32,
    ) {
        let alloc = Allocation {
            id,
            cpu_usage,
            memory_usage,
        };
        let vm = VirtualMachine::new(
            id,
            self.ctx.time(),
            lifetime,
            cpu_load_model,
            memory_load_model,
            self.sim_config.clone(),
        );
        self.ctx.emit_now(AllocationRequest { alloc, vm }, scheduler_id);
    }

    pub fn spawn_vm_with_delay(
        &mut self,
        id: u32,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        scheduler_id: u32,
        delay: f64,
    ) {
        let alloc = Allocation {
            id,
            cpu_usage,
            memory_usage,
        };
        let vm = VirtualMachine::new(
            id,
            self.ctx.time(),
            lifetime,
            cpu_load_model,
            memory_load_model,
            self.sim_config.clone(),
        );
        self.ctx.emit(AllocationRequest { alloc, vm }, scheduler_id, delay);
    }

    pub fn migrate_vm_to_host(&mut self, vm_id: u32, target_host: u32) {
        let mon = self.monitoring.borrow();
        let source_host = mon.find_host_by_vm(vm_id);
        let mut vm = mon.get_vm(vm_id);
        let alloc = mon.get_allocation(source_host, vm_id);
        vm.set_status(VmStatus::Initializing);
        self.ctx.emit(
            MigrationRequest { source_host, alloc, vm },
            target_host,
            self.sim_config.message_delay,
        );
    }

    pub fn build_custom_component<Component: 'static + CustomComponent>(
        &mut self,
        name: &str,
    ) -> Rc<RefCell<Component>> {
        let component = rc!(refcell!(Component::new(self.sim.create_context(name))));
        let id = self.sim.add_handler(name, component.clone());
        self.components.insert(id, component.clone());
        component
    }

    pub fn spawn_vms_from_dataset(&mut self, scheduler_id: u32, dataset: &mut dyn DatasetReader) {
        loop {
            let request_opt = dataset.get_next_vm();
            if request_opt.is_none() {
                break;
            }
            let request = request_opt.unwrap();

            self.spawn_vm_with_delay(
                request.id,
                request.cpu_usage,
                request.memory_usage,
                request.lifetime,
                Box::new(ConstLoadModel::new(1.0)),
                Box::new(ConstLoadModel::new(1.0)),
                scheduler_id,
                request.start_time,
            );
        }
    }

    pub fn monitoring(&self) -> Rc<RefCell<Monitoring>> {
        self.monitoring.clone()
    }

    pub fn context(&self) -> &SimulationContext {
        return &self.ctx;
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    pub fn event_count(&self) -> u64 {
        return self.sim.event_count();
    }

    pub fn current_time(&mut self) -> f64 {
        return self.sim.time();
    }

    pub fn host(&self, host_id: u32) -> Rc<RefCell<HostManager>> {
        self.hosts.get(&host_id).unwrap().clone()
    }

    pub fn vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        rc!(refcell!(self.monitoring.borrow().get_vm(vm_id).clone()))
    }

    pub fn vm_status(&self, vm_id: u32) -> VmStatus {
        self.monitoring.borrow().get_vm_status(vm_id).clone()
    }

    pub fn sim_config(&self) -> Rc<SimulationConfig> {
        self.sim_config.clone()
    }
}
