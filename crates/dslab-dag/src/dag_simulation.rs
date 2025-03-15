//! Simulation configuration and execution.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_compute::multicore::{Compute, CoresDependency};
use simcore::simulation::Simulation;

use crate::dag::DAG;
use crate::network::NetworkConfig;
use crate::resource::{Resource, ResourceConfig};
use crate::runner::{Config, DAGRunner, Start};
use crate::scheduler::Scheduler;
use crate::task::ResourceRestriction;

/// Provides a convenient API for configuring and running simulations of DAG execution.
pub struct DagSimulation {
    pub sim: Simulation,
    resource_configs: Vec<ResourceConfig>,
    network_config: NetworkConfig,
    scheduler: Rc<RefCell<dyn Scheduler>>,
    config: Config,
}

impl DagSimulation {
    /// Creates simulation with provided random seed, network model, scheduler and config.
    pub fn new(
        seed: u64,
        resources: Vec<ResourceConfig>,
        network_config: NetworkConfig,
        scheduler: Rc<RefCell<dyn Scheduler>>,
        config: Config,
    ) -> Self {
        DagSimulation {
            sim: Simulation::new(seed),
            resource_configs: resources,
            network_config,
            scheduler,
            config,
        }
    }

    /// Adds a resource with provided parameters and zero price.
    pub fn add_resource(&mut self, name: &str, speed: f64, cores: u32, memory: u64) {
        self.resource_configs.push(ResourceConfig {
            name: name.to_string(),
            speed,
            cores,
            memory,
            price: 0.0,
        });
    }

    /// Adds a resource with provided parameters.
    pub fn add_resource_with_price(&mut self, name: &str, speed: f64, cores: u32, memory: u64, price: f64) {
        self.resource_configs.push(ResourceConfig {
            name: name.to_string(),
            speed,
            cores,
            memory,
            price,
        });
    }

    /// Initializes DAG simulation.
    pub fn init(&mut self, mut dag: DAG) -> Rc<RefCell<DAGRunner>> {
        let net_ctx = self.sim.create_context("net");
        let network = Rc::new(RefCell::new(self.network_config.make_network(net_ctx)));
        self.sim.add_handler("net", network.clone());
        if !self.resource_configs.iter().any(|r| r.name == "master") {
            self.add_resource("master", 1., 1, 0);
        }
        let resources = self
            .resource_configs
            .iter()
            .map(|r| {
                let compute = Rc::new(RefCell::new(Compute::new(
                    r.speed,
                    r.cores,
                    r.memory,
                    self.sim.create_context(&r.name),
                )));
                let id = self.sim.add_handler(&r.name, compute.clone());
                Resource {
                    id,
                    name: r.name.clone(),
                    compute,
                    speed: r.speed,
                    cores: r.cores,
                    cores_available: r.cores,
                    memory: r.memory,
                    memory_available: r.memory,
                    price: r.price,
                }
            })
            .collect::<Vec<_>>();

        self.add_input_output_tasks(&mut dag);

        let runner = Rc::new(RefCell::new(DAGRunner::new(
            dag,
            network.clone(),
            resources.clone(),
            self.scheduler.clone(),
            self.config.clone(),
            self.sim.create_context("runner"),
        )));
        let runner_id = self.sim.add_handler("runner", runner.clone());
        self.network_config.init_network(network, runner_id, &resources);
        let client = self.sim.create_context("client");
        client.emit_now(Start {}, runner_id);
        runner
    }

    /// Performs the specified number of steps through the simulation.
    ///
    /// See [Simulation::steps()](simcore::simulation::Simulation::steps).
    pub fn steps(&mut self, step_count: u64) -> bool {
        self.sim.steps(step_count)
    }

    /// Steps through the simulation with duration limit.
    ///
    /// See [Simulation::step_for_duration()](simcore::simulation::Simulation::step_for_duration).
    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    /// Steps through the simulation until there are no pending events left.
    ///
    /// See [Simulation::step_until_no_events()](simcore::simulation::Simulation::step_until_no_events).
    pub fn step_until_no_events(&mut self) {
        self.sim.step_until_no_events();
    }

    /// Returns the total number of created events.
    ///
    /// See [Simulation::event_count()](simcore::simulation::Simulation::event_count).
    pub fn event_count(&self) -> u64 {
        self.sim.event_count()
    }

    /// Returns the current simulation time.
    ///
    /// See [Simulation::time()](simcore::simulation::Simulation::time).
    pub fn time(&mut self) -> f64 {
        self.sim.time()
    }

    fn add_input_output_tasks(&mut self, dag: &mut DAG) {
        let master_resource = self.resource_configs.iter().position(|r| r.name == "master").unwrap();

        for task in 0..dag.get_tasks().len() {
            dag.add_resource_restriction(task, ResourceRestriction::Except([master_resource].into()));
        }
        if !dag.get_inputs().is_empty() {
            let inputs = dag.get_inputs().clone();
            let input_task = dag.add_task("input", 0., 0, 1, 1, CoresDependency::Linear);
            dag.add_resource_restriction(input_task, ResourceRestriction::Only([master_resource].into()));
            for &input in inputs.iter() {
                dag.set_as_task_output(input, input_task);
            }
            dag.set_inputs(inputs);
        }
        if !dag.get_outputs().is_empty() {
            let outputs = dag.get_outputs().clone();
            let output_task = dag.add_task("output", 0., 0, 1, 1, CoresDependency::Linear);
            dag.add_resource_restriction(output_task, ResourceRestriction::Only([master_resource].into()));
            for &output in outputs.iter() {
                dag.add_data_dependency(output, output_task);
            }
            dag.set_outputs(outputs);
        }
    }
}
