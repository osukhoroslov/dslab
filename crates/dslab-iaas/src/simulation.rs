//! Simulation configuration and execution. Library API.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;
use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::simulation::Simulation;

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
use crate::core::vm_api::VmAPI;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;
use crate::custom_component::CustomComponent;
use crate::extensions::dataset_reader::DatasetReader;

/// Represents a simulation, provides methods for its configuration and execution.
pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    vm_api: Rc<RefCell<VmAPI>>,
    placement_store: Rc<RefCell<PlacementStore>>,
    hosts: BTreeMap<u32, Rc<RefCell<HostManager>>>,
    schedulers: HashMap<u32, Rc<RefCell<Scheduler>>>,
    components: HashMap<u32, Rc<RefCell<dyn CustomComponent>>>,
    sim: Simulation,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl CloudSimulation {
    /// Creates a simulation with specific config.
    pub fn new(mut sim: Simulation, sim_config: SimulationConfig) -> Self {
        let monitoring = rc!(refcell!(Monitoring::new(sim.create_context("monitoring"))));
        sim.add_handler("monitoring", monitoring.clone());

        let vm_api = rc!(refcell!(VmAPI::new(sim.create_context("vm_api"))));
        sim.add_handler("vm_api", vm_api.clone());

        let placement_store = rc!(refcell!(PlacementStore::new(
            sim_config.allow_vm_overcommit,
            vm_api.clone(),
            sim.create_context("placement_store"),
            sim_config.clone(),
        )));
        sim.add_handler("placement_store", placement_store.clone());

        let ctx = sim.create_context("simulation");
        Self {
            monitoring,
            vm_api,
            placement_store,
            hosts: BTreeMap::new(),
            schedulers: HashMap::new(),
            components: HashMap::new(),
            sim,
            ctx,
            sim_config: rc!(sim_config),
        }
    }

    /// Creates new host with specified CPU and RAM capacity. Returns a component ID.
    pub fn add_host(&mut self, name: &str, cpu_total: u32, memory_total: u64) -> u32 {
        // create host
        let host = rc!(refcell!(HostManager::new(
            cpu_total,
            memory_total,
            self.monitoring.borrow().get_id(),
            self.placement_store.borrow().get_id(),
            self.vm_api.clone(),
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

    /// Creates new scheduler with specified host selection algorithm. Returns a component ID.
    pub fn add_scheduler(&mut self, name: &str, vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>) -> u32 {
        // create scheduler using current state from placement store
        let pool_state = self.placement_store.borrow_mut().get_pool_state();
        let scheduler = rc!(refcell!(Scheduler::new(
            pool_state,
            self.monitoring.clone(),
            self.vm_api.clone(),
            self.placement_store.borrow().get_id(),
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

    /// Created a VM allocation request on specified scheduler. Request will be processed immediately.
    pub fn spawn_vm_now(
        &mut self,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        vm_id: Option<u32>,
        scheduler_id: u32,
    ) -> u32 {
        let id = vm_id.unwrap_or(self.vm_api.borrow_mut().generate_vm_id());
        let vm = VirtualMachine::new(
            id,
            cpu_usage,
            memory_usage,
            self.ctx.time(),
            lifetime,
            cpu_load_model,
            memory_load_model,
            self.sim_config.clone(),
        );
        self.vm_api.borrow_mut().register_new_vm(vm);
        self.ctx.emit_now(AllocationRequest { vm_id: id }, scheduler_id);
        id
    }

    /// Created a VM allocation request on specified scheduler with some delay.
    pub fn spawn_vm_with_delay(
        &mut self,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        vm_id: Option<u32>,
        scheduler_id: u32,
        delay: f64,
    ) -> u32 {
        let id = vm_id.unwrap_or(self.vm_api.borrow_mut().generate_vm_id());
        let vm = VirtualMachine::new(
            id,
            cpu_usage,
            memory_usage,
            self.ctx.time() + delay,
            lifetime,
            cpu_load_model,
            memory_load_model,
            self.sim_config.clone(),
        );
        self.vm_api.borrow_mut().register_new_vm(vm);
        self.ctx.emit(AllocationRequest { vm_id: id }, scheduler_id, delay);
        id
    }

    /// Migrate VM to specified host is there are enough space available on target host.
    pub fn migrate_vm_to_host(&mut self, vm_id: u32, target_host: u32) {
        let vm_api = self.vm_api.borrow();
        let source_host = vm_api.find_host_by_vm(vm_id);
        self.ctx.emit(
            MigrationRequest { source_host, vm_id },
            target_host,
            self.sim_config.message_delay,
        );
    }

    /// Add custom component to simulation.
    pub fn build_custom_component<Component: 'static + CustomComponent>(
        &mut self,
        name: &str,
    ) -> Rc<RefCell<Component>> {
        let component = rc!(refcell!(Component::new(self.sim.create_context(name))));
        let id = self.sim.add_handler(name, component.clone());
        self.components.insert(id, component.clone());
        component
    }

    /// Spawn all VMs from given dataset on specified scheduler.
    pub fn spawn_vms_from_dataset(&mut self, scheduler_id: u32, dataset: &mut dyn DatasetReader) {
        loop {
            let request_opt = dataset.get_next_vm();
            if request_opt.is_none() {
                break;
            }
            let request = request_opt.unwrap();

            self.spawn_vm_with_delay(
                request.cpu_usage,
                request.memory_usage,
                request.lifetime,
                Box::new(ConstLoadModel::new(1.0)),
                Box::new(ConstLoadModel::new(1.0)),
                Some(request.id),
                scheduler_id,
                request.start_time,
            );
        }
    }

    /// Get reference to monitoring component (actual host load).
    pub fn monitoring(&self) -> Rc<RefCell<Monitoring>> {
        self.monitoring.clone()
    }

    /// Get reference to VMAPI component (VM status, location, etc.).
    pub fn vm_api(&self) -> Rc<RefCell<VmAPI>> {
        self.vm_api.clone()
    }

    /// Get simulation context (to get current time).
    pub fn context(&self) -> &SimulationContext {
        return &self.ctx;
    }

    /// Process N simulation steps.
    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    /// Process simulation for "time" duration.
    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    /// Number of events processed yet.
    pub fn event_count(&self) -> u64 {
        return self.sim.event_count();
    }

    /// Get current simulation time.
    pub fn current_time(&mut self) -> f64 {
        return self.sim.time();
    }

    /// Get reference to host (host energy consumption, allocated resources etc.).
    pub fn host(&self, host_id: u32) -> Rc<RefCell<HostManager>> {
        self.hosts.get(&host_id).unwrap().clone()
    }

    /// Get reference to VM.
    pub fn vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        self.vm_api.borrow().get_vm(vm_id)
    }

    /// Get VM status (running, initializing, finished, etc.).
    pub fn vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_api.borrow().get_vm_status(vm_id)
    }

    /// Get VM location host ID.
    pub fn vm_location(&self, vm_id: u32) -> u32 {
        self.vm_api.borrow().find_host_by_vm(vm_id)
    }

    /// Get simulation config.
    pub fn sim_config(&self) -> Rc<SimulationConfig> {
        self.sim_config.clone()
    }
}
