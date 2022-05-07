use std::cell::RefCell;
use std::rc::Rc;

use compute::multicore::Compute;
use network::model::NetworkModel;
use network::network::Network;
use simcore::simulation::Simulation;

use crate::dag::DAG;
use crate::resource::{load_resources, Resource};
use crate::runner::{DAGRunner, Start};
use crate::scheduler::{Config, Scheduler};

pub struct DagSimulation {
    pub sim: Simulation,
    resources: Vec<Resource>,
    network_model: Rc<RefCell<dyn NetworkModel>>,
    scheduler: Rc<RefCell<dyn Scheduler>>,
    config: Config,
}

impl DagSimulation {
    pub fn new(
        seed: u64,
        network_model: Rc<RefCell<dyn NetworkModel>>,
        scheduler: Rc<RefCell<dyn Scheduler>>,
        config: Config,
    ) -> Self {
        DagSimulation {
            sim: Simulation::new(seed),
            resources: Vec::new(),
            network_model,
            scheduler,
            config,
        }
    }

    pub fn add_resource(&mut self, name: &str, speed: u64, cores: u32, memory: u64) {
        let compute = Rc::new(RefCell::new(Compute::new(
            speed,
            cores,
            memory,
            self.sim.create_context(&name),
        )));
        let id = self.sim.add_handler(&name, compute.clone());
        self.resources.push(Resource {
            id,
            name: name.to_string(),
            compute,
            speed: speed,
            cores_available: cores,
            memory_available: memory,
        });
    }

    pub fn load_resources(&mut self, filename: &str) {
        self.resources = load_resources(filename, &mut self.sim);
    }

    pub fn init(&mut self, dag: DAG) -> Rc<RefCell<DAGRunner>> {
        let network = Rc::new(RefCell::new(Network::new(
            self.network_model.clone(),
            self.sim.create_context("net"),
        )));
        self.sim.add_handler("net", network.clone());
        let runner = Rc::new(RefCell::new(DAGRunner::new(
            dag,
            network,
            self.resources.clone(),
            self.scheduler.clone(),
            self.config.clone(),
            self.sim.create_context("runner"),
        )));
        let runner_id = self.sim.add_handler("runner", runner.clone());
        let mut client = self.sim.create_context("client");
        client.emit_now(Start {}, runner_id);
        runner
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        return self.sim.steps(step_count);
    }

    pub fn step_for_duration(&mut self, time: f64) {
        self.sim.step_for_duration(time);
    }

    pub fn step_until_no_events(&mut self) {
        self.sim.step_until_no_events();
    }

    pub fn event_count(&self) -> u64 {
        return self.sim.event_count();
    }

    pub fn time(&mut self) -> f64 {
        return self.sim.time();
    }
}
