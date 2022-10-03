//! The main entry point for simulation configuration and execution.

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
use crate::core::power_model::LinearPowerModel;
use crate::core::power_model::PowerModel;
use crate::core::scheduler::Scheduler;
use crate::core::slav_model::SLATAHModel;
use crate::core::slav_model::SLAVModel;
use crate::core::vm::{VirtualMachine, VmStatus};
use crate::core::vm_api::VmAPI;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;
use crate::custom_component::CustomComponent;
use crate::extensions::dataset_reader::DatasetReader;

/// Represents a simulation, provides methods for its configuration and execution.
///
/// It encapsulates all simulation components and provides convenient access to them for the user.
pub struct CloudSimulation {
    monitoring: Rc<RefCell<Monitoring>>,
    vm_api: Rc<RefCell<VmAPI>>,
    placement_store: Rc<RefCell<PlacementStore>>,
    hosts: BTreeMap<u32, Rc<RefCell<HostManager>>>,
    schedulers: HashMap<u32, Rc<RefCell<Scheduler>>>,
    components: HashMap<u32, Rc<RefCell<dyn CustomComponent>>>,
    energy_model: Box<dyn PowerModel>,
    slav_model: Box<dyn SLAVModel>,
    sim: Simulation,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl CloudSimulation {
    /// Creates a simulation with specified config.
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
            energy_model: Box::new(LinearPowerModel::new(1.)),
            slav_model: Box::new(SLATAHModel::new()),
            sim,
            ctx,
            sim_config: rc!(sim_config),
        }
    }

    /// Creates new host with specified name and resource capacity, and returns the host ID.
    pub fn add_host(&mut self, name: &str, cpu_total: u32, memory_total: u64) -> u32 {
        // create host
        let host = rc!(refcell!(HostManager::new(
            cpu_total,
            memory_total,
            self.monitoring.borrow().get_id(),
            self.placement_store.borrow().get_id(),
            self.vm_api.clone(),
            self.sim_config.allow_vm_overcommit,
            self.energy_model.clone(),
            self.slav_model.clone(),
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

    /// Creates new scheduler with specified name and VM placement algorithm, and returns the scheduler ID.
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

    /// Creates new VM with specified properties, registers it in VM API and immediately submits the allocation request
    /// to the specified scheduler. Returns VM ID.
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

    /// Creates new VM with specified properties, registers it in VM API and submits the allocation request
    /// to the specified scheduler with the specified delay. Returns VM ID.
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

    /// Sends VM migration request to the specified target host.
    pub fn migrate_vm_to_host(&mut self, vm_id: u32, target_host: u32) {
        let vm_api = self.vm_api.borrow();
        let source_host = vm_api.find_host_by_vm(vm_id);
        self.ctx.emit(
            MigrationRequest { source_host, vm_id },
            target_host,
            self.sim_config.message_delay,
        );
    }

    /// Creates custom component and adds it to the simulation.
    pub fn build_custom_component<Component: 'static + CustomComponent>(
        &mut self,
        name: &str,
    ) -> Rc<RefCell<Component>> {
        let component = rc!(refcell!(Component::new(self.sim.create_context(name))));
        let id = self.sim.add_handler(name, component.clone());
        self.components.insert(id, component.clone());
        component
    }

    /// Spawns all VMs from the given dataset on the specified scheduler.
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

<<<<<<< HEAD
=======
    /// Overrides the used host energy load model.
    ///
    /// Should be called before adding hosts to simulation.
    pub fn set_energy_load_model(&mut self, energy_model: Box<dyn PowerModel>) {
        self.energy_model = energy_model;
    }

    /// Overrides the used SLAV model.
    ///
    /// Should be called before adding hosts to simulation.
    pub fn set_slav_model(&mut self, slav_model: Box<dyn SLAVModel>) {
        self.slav_model = slav_model;
    }

>>>>>>> Up to 2nd review
    /// Returns the reference to monitoring component (provides actual host load).
    pub fn monitoring(&self) -> Rc<RefCell<Monitoring>> {
        self.monitoring.clone()
    }

    /// Returns the reference to VM API component (provides information about VMs).
    pub fn vm_api(&self) -> Rc<RefCell<VmAPI>> {
        self.vm_api.clone()
    }

    /// Returns the main simulation context.
    pub fn context(&self) -> &SimulationContext {
        return &self.ctx;
    }

    /// Performs the specified number of steps through the simulation (see dslab-core docs).
    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    /// Steps through the simulation with duration limit (see dslab-core docs).
    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    /// Returns the total number of created events.
    pub fn event_count(&self) -> u64 {
        return self.sim.event_count();
    }

    /// Returns the current simulation time.
    pub fn current_time(&mut self) -> f64 {
        return self.sim.time();
    }

    /// Returns the reference to host manager (host energy consumption, allocated resources etc.).
    pub fn host(&self, host_id: u32) -> Rc<RefCell<HostManager>> {
        self.hosts.get(&host_id).unwrap().clone()
    }

    /// Returns the reference to VM information.
    pub fn vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        self.vm_api.borrow().get_vm(vm_id)
    }

    /// Returns the (possibly slightly outdated) status of specified VM via VM API.
    pub fn vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_api.borrow().get_vm_status(vm_id)
    }

    /// Returns the ID of host that runs the specified VM.
    pub fn vm_location(&self, vm_id: u32) -> u32 {
        self.vm_api.borrow().find_host_by_vm(vm_id)
    }

    /// Returns the simulation config.
    pub fn sim_config(&self) -> Rc<SimulationConfig> {
        self.sim_config.clone()
    }
}
