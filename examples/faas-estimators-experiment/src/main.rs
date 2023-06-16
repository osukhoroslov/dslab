use std::boxed::Box;
use std::path::Path;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::{Config, HostConfig};
use dslab_faas::cpu::IgnoredCpuPolicy;
use dslab_faas::extra::azure_trace::{process_azure_trace, AppPreference, AzureTraceConfig, AzureTrace, StartGenerator};
use dslab_faas::scheduler::LeastLoadedScheduler;
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::trace::{ApplicationData, RequestData, Trace};
use dslab_faas_estimators::benders_estimator::{BendersConfig, BendersLowerEstimator};
use dslab_faas_estimators::estimator::{Estimation, Estimator};
use dslab_faas_estimators::local_search_estimator::LocalSearchEstimator;
use dslab_faas_estimators::lp_lower_estimator::LpLowerEstimator;
use dslab_faas_estimators::ls::accept::SimulatedAnnealingAcceptanceCriterion;
use dslab_faas_estimators::ls::annealing::ExponentialAnnealingSchedule;
use dslab_faas_estimators::ls::common::OptimizationGoal;
use dslab_faas_estimators::ls::initial::GreedyInitialSolutionGenerator;
use dslab_faas_estimators::ls::local_search::LocalSearch;
use dslab_faas_estimators::ls::neighborhood::DestroyRepairNeighborhood;
use dslab_faas_estimators::path_cover_estimator::PathCoverEstimator;
use dslab_faas_estimators::segment_lower_estimator::SegmentLowerEstimator;

const ROUND: f64 = 100.0;

fn run(arg: &str, apps: Vec<AppPreference>) -> Vec<(f64, f64)> {
    let mut result = Vec::new();
    for i in 0..10 {
        let mut trace_config = AzureTraceConfig {
            time_period: 30,
            time_skip: 30 * i,
            start_generator: StartGenerator::PoissonFit,
            rps: Some(0.25),
            app_preferences: apps.clone(),
            .. Default::default()
        };
        let mut trace = Box::new(process_azure_trace(Path::new(arg), trace_config));
        /*if trace.request_iter().count() > 480 * 60 {
            trace_config = AzureTraceConfig {
                time_period: 480,
                time_skip: 480 * i,
                start_generator: StartGenerator::PoissonFit,
                rps: Some(1.),
                app_preferences: apps.clone(),
                .. Default::default()
            };
            trace = Box::new(process_azure_trace(Path::new(arg), trace_config));
        }*/
        println!("found {} reqs", trace.request_iter().count());
        let mut config1: Config = Default::default();
        let mut config2: Config = Default::default();
        config1.cpu_policy = Box::<IgnoredCpuPolicy>::default();
        config1.scheduler = Box::new(LeastLoadedScheduler::new(true, true, true));
        config1.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(10.0 * 60.0, 0.0));
        config2.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(10.0 * 60.0, 0.0));
        for _ in 0..3 {
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
        sim.load_trace(trace.as_ref());
        sim.step_until_no_events();
        let coldstart1 = sim.stats().global_stats.invocation_stats.cold_start_latency.sum();
        println!("Simulation coldstart latency = {}", coldstart1);
        //let mut up_est = AntColonyEstimator::new(AntColony::new(1), 10.0 * 60.0, 1000.);
        let mut low_est = BendersLowerEstimator::new(10.0 * 60.0, ROUND, BendersConfig { iterations: 30, max_cuts: 3000 });//PathCoverEstimator::new(10.0 * 60.0, 1000.);
        let mut low_est2 = SegmentLowerEstimator::new(10.0 * 60.0, ROUND);
        //let mut tmp_est = PathCoverEstimator::new(10.0 * 60.0, ROUND);
        let mut upper = 0.;
        let mut lower = 0.;
        /*if let Estimation::UpperBound(x) = up_est.estimate(&config2, trace.as_ref()) {
            println!("Upper coldstart latency = {}", x);
            upper = x;
        } else {
            panic!("wtf");
        }*/
        if let Estimation::LowerBound(x) = low_est.estimate(&config2, trace.as_ref()) {
            println!("Lower coldstart latency = {}", x);
            lower = x;
        } else {
            panic!("wtf");
        }
        /*if let Estimation::LowerBound(x) = tmp_est.estimate(&config2, trace.as_ref()) {
            println!("Path cover latency = {}", x);
        } else {
            panic!("wtf");
        }*/
        if let Estimation::LowerBound(x) = low_est2.estimate(&config2, trace.as_ref()) {
            println!("Lower coldstart latency (by resources) = {}", x);
            lower = x;
        } else {
            panic!("wtf");
        }
        result.push((lower, upper));
    }
    result
}

fn run_balanced(arg: &str) -> Vec<(f64, f64)> {
    run(arg, vec![AppPreference::new(100, 0.45, 0.55)])
}

fn run_skewed(arg: &str) -> Vec<(f64, f64)> {
    run(arg, vec![AppPreference::new(1, 0.05, 0.15), AppPreference::new(60, 0.45, 0.55)])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    //let mut v1 = run_balanced(&args[1]);
    let mut v2 = run_skewed(&args[1]);
    /*v1.extend(v2.drain(..));
    println!("Final bounds:");
    for (l, u) in v1.drain(..) {
        print!("({}, {}) ", l, u);
    }
    println!("");*/
}
