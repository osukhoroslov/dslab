//! The main entry point for simulation configuration and execution.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::context::SimulationContext;
use dslab_core::simulation::Simulation;
use dslab_core::Id;
use dslab_models::power::cpu_models::linear::LinearCpuPowerModel;
use dslab_models::power::host::{HostPowerModel, HostPowerModelBuilder};

use crate::core::config::sim_config::SimulationConfig;
use crate::core::events::allocation::{AllocationRequest, MigrationRequest};
use crate::core::host_manager::HostManager;
use crate::core::host_manager::SendHostState;
use crate::core::logger::{Logger, StdoutLogger};
use crate::core::monitoring::Monitoring;
use crate::core::placement_store::PlacementStore;
use crate::core::scheduler::Scheduler;
use crate::core::slav_metric::HostSLAVMetric;
use crate::core::slav_metric::OverloadTimeFraction;
use crate::core::vm::{ResourceConsumer, VirtualMachine, VmStatus};
use crate::core::vm_api::VmAPI;
use crate::core::vm_placement_algorithm::placement_algorithm_resolver;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;
use crate::custom_component::CustomComponent;
use crate::extensions::azure_dataset_reader::AzureDatasetReader;
use crate::extensions::dataset_reader::DatasetReader;
use crate::extensions::dataset_type::VmDatasetType;
use crate::extensions::huawei_dataset_reader::HuaweiDatasetReader;

struct VMSpawnRequest {
    pub resource_consumer: ResourceConsumer,
    pub lifetime: f64,
    pub vm_id: u32,
    pub scheduler_id: u32,
}

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
    batch_mode: bool,
    batch_buffer: Vec<VMSpawnRequest>,
    logger: Rc<RefCell<Box<dyn Logger>>>,
    sim: Simulation,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl CloudSimulation {
    /// Creates a simulation with specified config.
    pub fn new(sim: Simulation, sim_config: SimulationConfig) -> Self {
        CloudSimulation::with_logger(sim, sim_config, Box::new(StdoutLogger::new()))
    }

    /// Creates a simulation with specified config.
    pub fn with_logger(mut sim: Simulation, sim_config: SimulationConfig, logger: Box<dyn Logger>) -> Self {
        let logger: Rc<RefCell<Box<dyn Logger>>> = rc!(refcell!(logger));

        let monitoring = rc!(refcell!(Monitoring::new(
            sim.create_context("monitoring"),
            logger.clone()
        )));
        sim.add_handler("monitoring", monitoring.clone());

        let vm_api = rc!(refcell!(VmAPI::new(sim.create_context("vm_api"))));
        sim.add_handler("vm_api", vm_api.clone());

        let placement_store = rc!(refcell!(PlacementStore::new(
            sim_config.allow_vm_overcommit,
            vm_api.clone(),
            sim.create_context("placement_store"),
            logger.clone(),
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
            host_power_model: HostPowerModelBuilder::new()
                .cpu(Box::new(LinearCpuPowerModel::new(0.4, 1.)))
                .build(),
            slav_metric: Box::new(OverloadTimeFraction::new()),
            batch_mode: false,
            batch_buffer: Vec::new(),
            logger,
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

        // Spawn VM from specifyed dataset
        if sim.sim_config.trace.is_some() {
            let dataset_config = sim.sim_config.trace.as_ref().unwrap();

            match dataset_config.r#type {
                VmDatasetType::Azure => {
                    let mut dataset = AzureDatasetReader::new(
                        sim.sim_config.simulation_length,
                        sim.sim_config.hosts.first().unwrap().cpus as f64,
                        sim.sim_config.hosts.first().unwrap().memory as f64,
                    );
                    dataset.parse(
                        format!("{}/vm_types.csv", dataset_config.path),
                        format!("{}/vm_instances.csv", dataset_config.path),
                    );
                    sim.spawn_vms_from_dataset(*sim.schedulers.iter().next().unwrap().0, &mut dataset);
                }
                VmDatasetType::Huawei => {
                    let mut dataset = HuaweiDatasetReader::new(sim.sim_config.simulation_length);
                    dataset.parse(format!("{}/Huawei-East-1.csv", dataset_config.path));
                    sim.spawn_vms_from_dataset(*sim.schedulers.iter().next().unwrap().0, &mut dataset);
                }
            }
        }

        sim
    }

    fn add_host_internal(&mut self, name: &str, cpu_total: u32, memory_total: u64, rack_id: Option<u32>) -> u32 {
        // create host
        let host = rc!(refcell!(HostManager::new(
            rack_id,
            cpu_total,
            memory_total,
            self.monitoring.borrow().get_id(),
            self.placement_store.borrow().get_id(),
            self.vm_api.clone(),
            self.sim_config.allow_vm_overcommit,
            self.host_power_model.clone(),
            self.slav_metric.clone(),
            self.sim.create_context(name),
            self.logger.clone(),
            self.sim_config.clone(),
        )));
        let id = self.sim.add_handler(name, host.clone());
        self.hosts.insert(id, host);
        // add host to monitoring
        self.monitoring.borrow_mut().add_host(id, cpu_total, memory_total);
        // add host to placement store
        self.placement_store
            .borrow_mut()
            .add_host(id, cpu_total, memory_total, rack_id);
        // add host to schedulers
        for scheduler in self.schedulers.values() {
            scheduler.borrow_mut().add_host(id, cpu_total, memory_total, rack_id);
        }
        // start sending host state to monitoring
        self.ctx.emit_now(SendHostState {}, id);
        id
    }

    /// Creates new host with specified name and resource capacity, and returns the host ID.
    pub fn add_host(&mut self, name: &str, cpu_total: u32, memory_total: u64) -> u32 {
        self.add_host_internal(name, cpu_total, memory_total, None)
    }

    /// Creates new host with specified name and resource capacity, and returns the host ID.
    /// Also associates the host with the specified rack.
    pub fn add_host_in_rack(&mut self, name: &str, cpu_total: u32, memory_total: u64, rack_id: u32) -> u32 {
        self.add_host_internal(name, cpu_total, memory_total, Some(rack_id))
    }

    /// Creates new scheduler with specified name and VM placement algorithm, and returns the scheduler ID.
    pub fn add_scheduler(&mut self, name: &str, vm_placement_algorithm: VMPlacementAlgorithm) -> u32 {
        // create scheduler using current state from placement store
        let pool_state = self.placement_store.borrow_mut().get_pool_state();
        let scheduler = rc!(refcell!(Scheduler::new(
            pool_state,
            self.monitoring.clone(),
            self.vm_api.clone(),
            self.placement_store.borrow().get_id(),
            vm_placement_algorithm,
            self.sim.create_context(name),
            self.logger.clone(),
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
        resource_consumer: ResourceConsumer,
        lifetime: f64,
        vm_id: Option<u32>,
        scheduler_id: u32,
    ) -> u32 {
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
        if self.batch_mode {
            self.batch_buffer.push(VMSpawnRequest {
                resource_consumer,
                lifetime,
                vm_id: id,
                scheduler_id,
            });
        } else {
            let vm = VirtualMachine::new(
                id,
                self.ctx.time(),
                lifetime,
                resource_consumer,
                self.sim_config.clone(),
            );
            self.vm_api.borrow_mut().register_new_vm(vm);
            self.ctx.emit_now(AllocationRequest { vm_ids: vec![id] }, scheduler_id);
        }
        id
    }

    /// Creates new VM with specified properties, registers it in VM API and submits the allocation request
    /// to the specified scheduler with the specified delay. Returns VM ID.
    pub fn spawn_vm_with_delay(
        &mut self,
        resource_consumer: ResourceConsumer,
        lifetime: f64,
        vm_id: Option<u32>,
        scheduler_id: u32,
        delay: f64,
    ) -> u32 {
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
        let vm = VirtualMachine::new(
            id,
            self.ctx.time() + delay,
            lifetime,
            resource_consumer,
            self.sim_config.clone(),
        );
        self.vm_api.borrow_mut().register_new_vm(vm);
        self.ctx
            .emit(AllocationRequest { vm_ids: vec![id] }, scheduler_id, delay);
        id
    }

    /// Creates new VM with specified properties and spawns it on the specified host bypassing the scheduling step.
    /// This is useful for creating the initial resource pool state.
    pub fn spawn_vm_on_host(
        &mut self,
        resource_consumer: ResourceConsumer,
        lifetime: f64,
        vm_id: Option<u32>,
        host_id: u32,
    ) -> u32 {
        let id = vm_id.unwrap_or_else(|| self.vm_api.borrow_mut().generate_vm_id());
        let vm = VirtualMachine::new(
            id,
            self.ctx.time(),
            lifetime,
            resource_consumer,
            self.sim_config.clone(),
        );
        self.vm_api.borrow_mut().register_new_vm(vm);
        self.placement_store
            .borrow_mut()
            .direct_allocation_commit(vec![id], vec![host_id]);
        id
    }

    /// Switches API to batch mode for building multi-VM requests.
    /// The subsequent invocations of `spawn_vm_now` will not spawn VM immediately but will add it to the batch.
    /// After the batch is completed it can be submitted using the `spawn_batch` method.
    pub fn begin_batch(&mut self) {
        assert!(!self.batch_mode, "Batch mode is already enabled");
        self.batch_mode = true;
    }

    /// Spawns the current batch as a single multi-VM requests and disables the batch mode.
    /// Returns the IDs of spawned VMs.
    pub fn spawn_batch(&mut self) -> Vec<u32> {
        assert!(self.batch_mode, "Batch mode is not enabled");
        assert!(!self.batch_buffer.is_empty(), "Batch buffer is empty");
        let mut vm_ids = Vec::new();
        let scheduler_id = self.batch_buffer[0].scheduler_id;
        for req in self.batch_buffer.drain(..) {
            let vm = VirtualMachine::new(
                req.vm_id,
                self.ctx.time(),
                req.lifetime,
                req.resource_consumer,
                self.sim_config.clone(),
            );
            self.vm_api.borrow_mut().register_new_vm(vm);
            vm_ids.push(req.vm_id);
            assert_eq!(
                req.scheduler_id, scheduler_id,
                "Requests in batch have different scheduler ids"
            );
        }
        self.ctx
            .emit_now(AllocationRequest { vm_ids: vm_ids.clone() }, scheduler_id);
        self.batch_mode = false;
        vm_ids
    }

    /// Sends VM migration request to the specified target host.
    pub fn migrate_vm_to_host(&mut self, vm_id: u32, target_host: u32) {
        let vm_api = self.vm_api.borrow();
        let source_host = vm_api
            .find_host_by_vm(vm_id)
            .unwrap_or_else(|| panic!("Cannot migrate VM {}: source host is not found", vm_id));
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
            if request.scheduler_name.is_some() {
                scheduler_id = self.sim.lookup_id(&request.scheduler_name.unwrap());
            }

            self.spawn_vm_with_delay(
                ResourceConsumer::new(
                    request.cpu_usage,
                    request.memory_usage,
                    request.cpu_load_model.clone(),
                    request.memory_load_model.clone(),
                ),
                request.lifetime,
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
        &self.ctx
    }

    /// Performs the specified number of steps through the simulation (see dslab-core docs).
    pub fn steps(&mut self, step_count: u64) -> bool {
        self.sim.steps(step_count)
    }

    /// Steps through the simulation with duration limit (see dslab-core docs).
    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    /// Steps through the simulation until the specified time (see dslab-core docs).
    pub fn step_until_time(&mut self, time: f64) {
        self.sim.step_until_time(time);
    }

    /// Returns the total number of created events.
    pub fn event_count(&self) -> u64 {
        self.sim.event_count()
    }

    /// Returns the current simulation time.
    pub fn current_time(&mut self) -> f64 {
        self.sim.time()
    }

    /// Returns the map with references to host managers.
    pub fn hosts(&self) -> BTreeMap<u32, Rc<RefCell<HostManager>>> {
        self.hosts.clone()
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
    pub fn vm_location(&self, vm_id: u32) -> Option<u32> {
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

    /// Returns the average CPU load across all hosts.
    pub fn average_cpu_load(&mut self) -> f64 {
        let mut sum_cpu_load = 0.;
        let time = self.current_time();
        for host in self.hosts.values() {
            sum_cpu_load += host.borrow().cpu_load(time);
        }
        sum_cpu_load / (self.hosts.len() as f64)
    }

    /// Returns the average memory load across all hosts.
    pub fn average_memory_load(&mut self) -> f64 {
        let mut sum_memory_load = 0.;
        let time = self.current_time();
        for host in self.hosts.values() {
            sum_memory_load += host.borrow().memory_load(time);
        }
        sum_memory_load / (self.hosts.len() as f64)
    }

    /// Returns the current CPU allocation rate (% of overall CPU used).
    pub fn cpu_allocation_rate(&mut self) -> f64 {
        let mut sum_cpu_allocated = 0.;
        let mut sum_cpu_total = 0.;
        for host in self.hosts.values() {
            sum_cpu_allocated += host.borrow().cpu_allocated();
            sum_cpu_total += host.borrow().cpu_total() as f64;
        }
        sum_cpu_allocated / sum_cpu_total
    }

    /// Returns the current memory allocation rate (% of overall RAM used).
    pub fn memory_allocation_rate(&mut self) -> f64 {
        let mut sum_memory_allocated = 0.;
        let mut sum_memory_total = 0.;
        for host in self.hosts.values() {
            sum_memory_allocated += host.borrow().memory_allocated();
            sum_memory_total += host.borrow().memory_total() as f64;
        }
        sum_memory_allocated / sum_memory_total
    }

    pub fn log_error(&mut self, log: String) {
        self.logger.borrow_mut().log_error(self.context(), log);
    }

    pub fn log_warn(&mut self, log: String) {
        self.logger.borrow_mut().log_warn(self.context(), log);
    }

    pub fn log_info(&mut self, log: String) {
        self.logger.borrow_mut().log_info(self.context(), log);
    }

    pub fn log_debug(&mut self, log: String) {
        self.logger.borrow_mut().log_debug(self.context(), log);
    }

    pub fn log_trace(&mut self, log: String) {
        self.logger.borrow_mut().log_trace(self.context(), log);
    }

    pub fn save_log(&self, path: &str) -> Result<(), std::io::Error> {
        self.logger.borrow_mut().save_log(path)
    }
}
