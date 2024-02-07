use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use threadpool::ThreadPool;

use dslab_core::context::SimulationContext;

use crate::dag::DAG;
use crate::dag_simulation::DagSimulation;
use crate::data_item::DataTransferMode;
use crate::network::NetworkConfig;
use crate::resource::ResourceConfig;
use crate::runner::Config;
use crate::run_stats::RunStats;
use crate::scheduler::{Action, Scheduler, SchedulerParams};
use crate::system::System;

/// This trait allows implementing multiobjective schedulers that find several schedules at once
/// (so-called Pareto front). These schedulers are always static.
pub trait ParetoScheduler {
    fn find_pareto_front(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Vec<Action>>;
}

pub struct ParetoRunResult {
    pub dag: String,
    pub system: String,
    pub scheduler: String,
    pub exec_time: f64,
    pub run_stats: Vec<RunStats>,
}

pub struct ParetoRun {
    dag_name: String,
    dag: DAG,
    system_name: String,
    resources: Vec<ResourceConfig>,
    network: NetworkConfig,
    scheduler: SchedulerParams,
    data_transfer_mode: DataTransferMode,
    scheduler_resolver: fn(&SchedulerParams) -> Option<Rc<RefCell<dyn ParetoScheduler>>>,
    pricing_interval: Option<f64>,
}

impl ParetoRun {
    /// Pareto schedulers are run as follows:
    /// 1) Run the scheduler in a fake simulation to produce Pareto front.
    /// 2) Run a separate simulation for each schedule in the Pareto front by using the stub scheduler.
    /// Note that since the simulation is fake and the scheduler is static, the scheduler should not
    /// emit any events or modify the simulation in any other way.
    pub fn run(&self, num_threads: usize) -> ParetoRunResult {
        let scheduler = (self.scheduler_resolver)(&self.scheduler).expect(&format!("Can't resolve Pareto scheduler from params {:?}", &self.scheduler));
        let config = Config {
            data_transfer_mode: self.data_transfer_mode.clone(),
            pricing_interval: self.pricing_interval.unwrap_or(1.0),
        };

        let mut fake_sim = DagSimulation::new(1, self.resources.clone(), self.network.clone(), Rc::new(RefCell::new(PredefinedActionsScheduler::new(Vec::new()))), config.clone());
        let fake_runner_rc = fake_sim.init(self.dag.clone());
        let fake_runner = fake_runner_rc.borrow();
        let fake_network = fake_runner.get_network();
        let system = System {
            resources: fake_runner.get_resources(),
            network: &fake_network.borrow(),
        };
        let start = Instant::now();
        let schedulers = scheduler.borrow_mut().find_pareto_front(&self.dag, system, config, fake_runner.get_context()).into_iter().map(|x| Box::new(PredefinedActionsScheduler::new(x))).collect::<Vec<_>>();
        let pool = ThreadPool::new(num_threads);
        let results = Arc::new(Mutex::new(Vec::new()));
        for scheduler_box in schedulers.into_iter() {
            let results = results.clone();
            let resources = self.resources.clone();
            let network = self.network.clone();
            let data_transfer_mode = self.data_transfer_mode.clone();
            let pricing_interval = self.pricing_interval.clone();
            let dag = self.dag.clone();
            pool.execute(move || {
                let scheduler = Rc::new(RefCell::new(*scheduler_box));

                let mut sim = DagSimulation::new(
                    123,
                    resources,
                    network,
                    scheduler,
                    Config {
                        data_transfer_mode: data_transfer_mode,
                        pricing_interval: pricing_interval.unwrap_or(1.0)
                    },
                );

                let runner = sim.init(dag);

                sim.step_until_no_events();

                runner.borrow().validate_completed();

                results.lock().unwrap().push(runner.borrow().run_stats().clone());
            });
        }
        ParetoRunResult {
            dag: self.dag_name.clone(),
            system: self.system_name.clone(),
            scheduler: self.scheduler.to_string(),
            exec_time: start.elapsed().as_secs_f64(),
            run_stats: Arc::try_unwrap(results).unwrap().into_inner().unwrap(),
        }
    }
}

pub struct PredefinedActionsScheduler {
    actions: Vec<Action>,
}

impl PredefinedActionsScheduler {
    pub fn new(actions: Vec<Action>) -> Self {
        Self { actions }
    }
}

impl Scheduler for PredefinedActionsScheduler {
    fn start(&mut self, _dag: &DAG, _system: System, _config: Config, _ctx: &SimulationContext) -> Vec<Action> {
        self.actions.clone()
    }

    fn is_static(&self) -> bool {
        true
    }
}
