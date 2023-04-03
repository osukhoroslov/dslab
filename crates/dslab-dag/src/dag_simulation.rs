//! Simulation configuration and execution.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_compute::multicore::Compute;
use dslab_core::simulation::Simulation;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::network::NetworkConfig;
use crate::resource::{Resource, ResourceConfig};
use crate::runner::{Config, DAGRunner, Start};
use crate::scheduler::Scheduler;

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

    /// Adds a resource with provided parameters.
    pub fn add_resource(&mut self, name: &str, speed: f64, cores: u32, memory: u64) {
        self.resource_configs.push(ResourceConfig {
            name: name.to_string(),
            speed,
            cores,
            memory,
        });
    }

    /// Initializes DAG simulation.
    pub fn init(&mut self, dag: DAG) -> Rc<RefCell<DAGRunner>> {
        let network = Rc::new(RefCell::new(Network::new(
            self.network_config
                .make_network()
                .expect("Failed to create network model"),
            self.sim.create_context("net"),
        )));
        self.sim.add_handler("net", network.clone());
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
                    cores_available: r.cores,
                    memory_available: r.memory,
                }
            })
            .collect::<Vec<_>>();
        let runner = Rc::new(RefCell::new(DAGRunner::new(
            dag,
            network,
            resources,
            self.scheduler.clone(),
            self.config.clone(),
            self.sim.create_context("runner"),
        )));
        let runner_id = self.sim.add_handler("runner", runner.clone());
        let mut client = self.sim.create_context("client");
        client.emit_now(Start {}, runner_id);
        runner
    }

    /// Performs the specified number of steps through the simulation.
    ///
    /// See [Simulation::steps()](dslab_core::simulation::Simulation::steps).
    pub fn steps(&mut self, step_count: u64) -> bool {
        self.sim.steps(step_count)
    }

    /// Steps through the simulation with duration limit.
    ///
    /// See [Simulation::step_for_duration()](dslab_core::simulation::Simulation::step_for_duration).
    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    /// Steps through the simulation until there are no pending events left.
    ///
    /// See [Simulation::step_until_no_events()](dslab_core::simulation::Simulation::step_until_no_events).
    pub fn step_until_no_events(&mut self) {
        self.sim.step_until_no_events();
    }

    /// Returns the total number of created events.
    ///
    /// See [Simulation::event_count()](dslab_core::simulation::Simulation::event_count).
    pub fn event_count(&self) -> u64 {
        self.sim.event_count()
    }

    /// Returns the current simulation time.
    ///
    /// See [Simulation::time()](dslab_core::simulation::Simulation::time).
    pub fn time(&mut self) -> f64 {
        self.sim.time()
    }
}
