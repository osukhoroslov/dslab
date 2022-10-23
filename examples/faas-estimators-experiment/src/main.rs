use std::boxed::Box;
use std::path::Path;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::{Config, HostConfig};
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas_estimators::estimator::{Estimation, Estimator};
use dslab_faas_estimators::local_search_estimator::LocalSearchEstimator;
use dslab_faas_estimators::ls::accept::SimulatedAnnealingAcceptanceCriterion;
use dslab_faas_estimators::ls::annealing::ExponentialAnnealingSchedule;
use dslab_faas_estimators::ls::common::OptimizationGoal;
use dslab_faas_estimators::ls::initial::GreedyInitialSolutionGenerator;
use dslab_faas_estimators::ls::local_search::LocalSearch;
use dslab_faas_estimators::ls::neighborhood::DestroyRepairNeighborhood;
use dslab_faas_extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas_extra::simple_schedulers::LeastLoadedScheduler;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut trace_config: AzureTraceConfig = Default::default();
    trace_config.invocations_limit = 5000;
    let trace = Box::new(process_azure_trace(Path::new(&args[1]), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let mut config1: Config = Default::default();
    let mut config2: Config = Default::default();
    config1.scheduler = Box::new(LeastLoadedScheduler::new(true));
    config1.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0));
    config2.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0));
    for _ in 0..10 {
        let mut host1: HostConfig = Default::default();
        host1.resources = vec![("mem".to_string(), 4096 * 4)];
        host1.cores = 4;
        config1.hosts.push(host1);
        let mut host2: HostConfig = Default::default();
        host2.resources = vec![("mem".to_string(), 4096 * 4)];
        host2.cores = 4;
        config2.hosts.push(host2);
    }
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config1);
    sim.load_trace(trace.as_ref());
    sim.step_until_no_events();
    let coldstart1 = sim.get_stats().cold_start_latency.sum();
    println!("Simulation coldstart latency = {}", coldstart1);
    let mut est = LocalSearchEstimator::new(
        OptimizationGoal::Minimization,
        LocalSearch::new(
            Box::new(SimulatedAnnealingAcceptanceCriterion::new(Box::new(
                ExponentialAnnealingSchedule::new(1000000.0, 0.99),
            ))),
            OptimizationGoal::Minimization,
            Box::new(GreedyInitialSolutionGenerator {}),
            Box::new(DestroyRepairNeighborhood::new(0.02)),
            1,
            2.0,
        ),
        20.0 * 60.0,
    );
    if let Estimation::UpperBound(x) = est.estimate(config2, trace) {
        println!("Estimated coldstart latency = {}", x);
    } else {
        assert!(false);
    }
}
