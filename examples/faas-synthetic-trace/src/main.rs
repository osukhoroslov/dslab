use std::boxed::Box;

use rand_distr::{Exp, LogNormal};

use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::{Config, HostConfig};
use dslab_faas::extra::synthetic_trace::{
    generate_synthetic_trace, ArrivalGenerator, DurationGenerator, MemoryGenerator, SyntheticTraceAppConfig,
    SyntheticTraceConfig,
};
use dslab_faas::simulation::ServerlessSimulation;
use simcore::simulation::Simulation;

fn main() {
    let apps = vec![
        SyntheticTraceAppConfig {
            activity_window: (0., 100.),
            arrival_generator: ArrivalGenerator::Random(Box::new(Exp::<f64>::new(1.).unwrap())),
            cold_start_latency: 0.1,
            concurrency_level: 1,
            cpu_share: 1.,
            duration_generator: DurationGenerator::Equal(0.1),
            memory_generator: MemoryGenerator::Fixed(128),
        },
        SyntheticTraceAppConfig {
            activity_window: (0., 100.),
            arrival_generator: ArrivalGenerator::Random(Box::new(Exp::<f64>::new(2.).unwrap())),
            cold_start_latency: 0.1,
            concurrency_level: 1,
            cpu_share: 1.,
            duration_generator: DurationGenerator::Random(Box::new(LogNormal::<f64>::new(-0.38, 2.36).unwrap())),
            memory_generator: MemoryGenerator::Fixed(128),
        },
    ];
    let trace_config = SyntheticTraceConfig {
        apps,
        memory_name: "mem".to_string(),
        random_seed: 1,
    };
    let trace = Box::new(generate_synthetic_trace(trace_config));
    let mut hosts = Vec::new();
    for _ in 0..8 {
        let host = HostConfig {
            resources: vec![("mem".to_string(), 4096)],
            cores: 4,
            ..Default::default()
        };
        hosts.push(host);
    }
    let config = Config {
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(5., 0., false)),
        hosts,
        ..Default::default()
    };
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    sim.load_trace(trace.as_ref());
    sim.step_until_no_events();
    sim.global_stats().print_summary("synthetic trace experiment");
}
