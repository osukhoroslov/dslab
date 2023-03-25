use std::boxed::Box;
use std::path::Path;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::{Config, HostConfig};
use dslab_faas::extra::azure_trace::{process_azure_trace, AppPreference, AzureTraceConfig, AzureTrace};
use dslab_faas::scheduler::LeastLoadedScheduler;
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::trace::{ApplicationData, RequestData, Trace};
use dslab_faas_estimators::ant::colony::AntColony;
use dslab_faas_estimators::ant_colony_estimator::AntColonyEstimator;
use dslab_faas_estimators::estimator::{Estimation, Estimator};
use dslab_faas_estimators::local_search_estimator::LocalSearchEstimator;
use dslab_faas_estimators::ls::accept::SimulatedAnnealingAcceptanceCriterion;
use dslab_faas_estimators::ls::annealing::ExponentialAnnealingSchedule;
use dslab_faas_estimators::ls::common::OptimizationGoal;
use dslab_faas_estimators::ls::initial::GreedyInitialSolutionGenerator;
use dslab_faas_estimators::ls::local_search::LocalSearch;
use dslab_faas_estimators::ls::neighborhood::DestroyRepairNeighborhood;

struct TraceWindowIter<'a> {
    inner: Box<dyn Iterator<Item = RequestData> + 'a>,
    l: f64,
    r: f64,
}

impl<'a> Iterator for TraceWindowIter<'a> {
    type Item = RequestData;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(req) = self.inner.next() {
            if req.time > self.l - 1e-12 && req.time < self.r - 1e-12 {
                return Some(req);
            }
        }
        None
    }
}

struct TraceWindow<'a> {
    len: usize,
    id: usize,
    azure_trace: &'a AzureTrace,
}

impl<'a> Trace for TraceWindow<'a> {
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_> {
        self.azure_trace.app_iter()
    }

    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_> {
        Box::new(TraceWindowIter { inner: self.azure_trace.request_iter(), l: (self.len * self.id * 60) as f64, r: (self.len * (self.id + 1) * 60) as f64})
    }

    fn function_iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        self.azure_trace.function_iter()
    }

    fn simulation_end(&self) -> Option<f64> {
        None
    }
}

fn run(arg: &str, apps: Vec<AppPreference>) -> Vec<f64> {
    let trace_config = AzureTraceConfig {
        time_period: 240,
        app_preferences: apps,
        .. Default::default()
    };
    let trace = Box::new(process_azure_trace(Path::new(arg), trace_config));
    let mut result = Vec::new();
    for i in 0..(240/240) {
        let window = TraceWindow { len: 240, id: i, azure_trace: &trace };
        let mut config1: Config = Default::default();
        let mut config2: Config = Default::default();
        config1.disable_contention = true;
        config1.scheduler = Box::new(LeastLoadedScheduler::new(true));
        config1.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0));
        config2.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0));
        for _ in 0..18 {
            let mut host1: HostConfig = Default::default();
            host1.resources = vec![("mem".to_string(), 4096)];
            host1.cores = 4;
            config1.hosts.push(host1);
            let mut host2: HostConfig = Default::default();
            host2.resources = vec![("mem".to_string(), 4096)];
            host2.cores = 4;
            config2.hosts.push(host2);
        }
        let mut sim = ServerlessSimulation::new(Simulation::new(1), config1);
        sim.load_trace(&window);
        sim.step_until_no_events();
        let coldstart1 = sim.get_stats().cold_start_latency.sum();
        println!("Simulation coldstart latency = {}", coldstart1);
        let mut est = AntColonyEstimator::new(AntColony::new(1), 20.0 * 60.0, 1000.);
        if let Estimation::UpperBound(x) = est.estimate(config2, &window) {
            println!("Estimated coldstart latency = {}", x);
            result.push(x);
        } else {
            panic!("wtf");
        }
    }
    result
}

fn run_balanced(arg: &str) -> Vec<f64> {
    run(arg, vec![AppPreference::new(100, 0.45, 0.55)])
}

fn run_skewed(arg: &str) -> Vec<f64> {
    run(arg, vec![AppPreference::new(1, 0.2, 0.3), AppPreference::new(60, 0.45, 0.55)])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut v1 = run_balanced(&args[1]);
    let mut v2 = run_skewed(&args[1]);
    v1.extend(v2.drain(..));
    println!("Final upper bounds:");
    for x in v1.drain(..) {
        print!("{} ", x);
    }
    println!("");
}
