use std::boxed::Box;

use rand_distr::{Distribution, Exp, LogNormal};
use rand_pcg::Pcg64;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::{Config, HostConfig};
use dslab_faas::extra::synthetic_trace::{
    generate_synthetic_trace, ArrivalGenerator, DistributionWrapper, DurationGenerator, MemoryGenerator,
    SyntheticTraceAppConfig, SyntheticTraceConfig,
};
use dslab_faas::simulation::ServerlessSimulation;

struct ExpWrapper {
    exp: Exp<f64>,
}

impl ExpWrapper {
    pub fn new(lambda: f64) -> Self {
        Self {
            exp: Exp::<f64>::new(lambda).unwrap(),
        }
    }
}

impl DistributionWrapper<f64, Pcg64> for ExpWrapper {
    fn sample(&self, rng: &mut Pcg64) -> f64 {
        self.exp.sample(rng)
    }
}

struct LogNormalWrapper {
    lognorm: LogNormal<f64>,
}

impl LogNormalWrapper {
    pub fn new(mu: f64, sigma: f64) -> Self {
        Self {
            lognorm: LogNormal::<f64>::new(mu, sigma).unwrap(),
        }
    }
}

impl DistributionWrapper<f64, Pcg64> for LogNormalWrapper {
    fn sample(&self, rng: &mut Pcg64) -> f64 {
        self.lognorm.sample(rng)
    }
}

fn main() {
    let apps = vec![
        SyntheticTraceAppConfig {
            activity_window: (0., 100.),
            arrival_generator: ArrivalGenerator::Random(Box::new(ExpWrapper::new(1.))),
            cold_start_latency: 0.1,
            concurrency_level: 1,
            cpu_share: 1.,
            duration_generator: DurationGenerator::Equal(0.1),
            memory_generator: MemoryGenerator::Fixed(128),
        },
        SyntheticTraceAppConfig {
            activity_window: (0., 100.),
            arrival_generator: ArrivalGenerator::Random(Box::new(ExpWrapper::new(2.))),
            cold_start_latency: 0.1,
            concurrency_level: 1,
            cpu_share: 1.,
            duration_generator: DurationGenerator::Random(Box::new(LogNormalWrapper::new(-0.38, 2.36))),
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
