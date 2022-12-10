//! The main entry point for simulation configuration and execution.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::simulation::Simulation;
use dslab_core::Id;

use crate::core::config::SimulationConfig;
use crate::core::events::allocation::{AllocationRequest, MigrationRequest};
use crate::core::host_manager::HostManager;
use crate::core::host_manager::SendHostState;
use crate::core::load_model::LoadModel;
use crate::core::monitoring::Monitoring;
use crate::core::placement_store::PlacementStore;
use crate::core::power_model::HostPowerModel;
use crate::core::power_model::LinearPowerModel;
use crate::core::scheduler::Scheduler;
use crate::core::slav_metric::HostSLAVMetric;
use crate::core::slav_metric::OverloadTimeFraction;
use crate::core::vm::{VirtualMachine, VmStatus};
use crate::core::vm_api::VmAPI;
use crate::core::vm_placement_algorithm::placement_algorithm_resolver;
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
    host_power_model: HostPowerModel,
    slav_metric: Box<dyn HostSLAVMetric>,
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
        let mut sim = Self {
            monitoring,
            vm_api,
            placement_store,
            hosts: BTreeMap::new(),
            schedulers: HashMap::new(),
            components: HashMap::new(),
            host_power_model: HostPowerModel::new(Box::new(LinearPowerModel::new(1., 0.4))).with_zero_idle_power(),
            slav_metric: Box::new(OverloadTimeFraction::new()),
            sim,
            ctx,
            sim_config: rc!(sim_config),
        };

        // Add hosts from config
        for host_config in sim.sim_config.hosts.clone() {
            let count = host_config.count.unwrap_or(1);
            if count == 1 {
                let name = host_config.name.unwrap();
                sim.add_host(&name, host_config.cpus, host_config.memory);
            } else {
                let prefix = host_config.name_prefix.unwrap();
                for i in 0..count {
                    let name = format!("{}{}", prefix, i + 1);
                    sim.add_host(&name, host_config.cpus, host_config.memory);
                }
            }
        }

        // Add schedulers from config
        for scheduler_config in sim.sim_config.schedulers.clone() {
            let count = scheduler_config.count.unwrap_or(1);
            if count == 1 {
                let name = scheduler_config.name.unwrap();
                let alg = placement_algorithm_resolver(scheduler_config.algorithm);
                sim.add_scheduler(&name, alg);
            } else {
                let prefix = scheduler_config.name_prefix.unwrap();
                for i in 0..scheduler_config.count.unwrap_or(1) {
                    let name = format!("{}{}", prefix, i + 1);
                    let alg = placement_algorithm_resolver(scheduler_config.algorithm.clone());
                    sim.add_scheduler(&name, alg);
                }
            }
        }

        sim
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
            self.host_power_model.clone(),
            self.slav_metric.clone(),
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
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
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
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
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

    /// Creates new VM with specified properties and spawns it on the specified host bypassing the scheduling step.
    /// This is useful for creating the initial resource pool state.
    pub fn spawn_vm_on_host(
        &mut self,
        cpu_usage: u32,
        memory_usage: u64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        vm_id: Option<u32>,
        host_id: u32,
    ) -> u32 {
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
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
        self.placement_store.borrow_mut().direct_allocation_commit(id, host_id);
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

    /// Spawns all VMs from the given dataset.
    ///
    /// The specified default scheduler is used for VM requests without scheduler information.
    pub fn spawn_vms_from_dataset(&mut self, default_scheduler_id: u32, dataset: &mut dyn DatasetReader) {
        loop {
            let request_opt = dataset.get_next_vm();
            if request_opt.is_none() {
                break;
            }
            let request = request_opt.unwrap();

            let mut scheduler_id = default_scheduler_id;
            if !request.scheduler_name.is_none() {
                scheduler_id = self.sim.lookup_id(&request.scheduler_name.unwrap());
            }

            self.spawn_vm_with_delay(
                request.cpu_usage,
                request.memory_usage,
                request.lifetime,
                request.cpu_load_model.clone(),
                request.memory_load_model.clone(),
                request.id,
                scheduler_id,
                request.start_time,
            );
        }
    }

    /// Overrides the used host power model.
    ///
    /// Should be called before adding hosts to simulation.
    pub fn set_host_power_model(&mut self, host_power_model: HostPowerModel) {
        self.host_power_model = host_power_model;
    }

    /// Overrides the used host-level SLAV metric.
    ///
    /// Should be called before adding hosts to simulation.
    pub fn set_slav_metric(&mut self, slav_metric: Box<dyn HostSLAVMetric>) {
        self.slav_metric = slav_metric;
    }

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

    /// Returns the reference to host manager (host energy consumption, allocated resources etc.).
    pub fn host_by_name(&self, name: &str) -> Rc<RefCell<HostManager>> {
        let host_id = self.sim.lookup_id(name);
        self.hosts.get(&host_id).unwrap().clone()
    }

    /// Returns the reference to scheduler.
    pub fn scheduler(&self, scheduler_id: u32) -> Rc<RefCell<Scheduler>> {
        self.schedulers.get(&scheduler_id).unwrap().clone()
    }

    /// Returns the reference to host scheduler.
    pub fn scheduler_by_name(&self, name: &str) -> Rc<RefCell<Scheduler>> {
        let scheduler_id = self.sim.lookup_id(name);
        self.schedulers.get(&scheduler_id).unwrap().clone()
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

    /// Returns the identifier of component by its name.
    pub fn lookup_id(&self, name: &str) -> Id {
        self.sim.lookup_id(name)
    }
}
