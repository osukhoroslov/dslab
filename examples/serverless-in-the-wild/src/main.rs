use std::boxed::Box;
use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};

use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas::extra::hybrid_histogram::HybridHistogramPolicy;
use dslab_faas::parallel::parallel_simulation_raw;
use dslab_faas::stats::Stats;

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub coldstart_policies: Vec<String>,
}

fn print_results(stats: Stats, name: &str) {
    println!("describing {}", name);
    println!("{} successful invocations", stats.invocations);
    println!(
        "- cold start rate = {}",
        (stats.cold_starts as f64) / (stats.invocations as f64)
    );
    println!(
        "- wasted memory time = {}",
        stats.wasted_resource_time.get(&0).unwrap().sum()
    );
    println!("- mean absolute total slowdown = {}", stats.abs_total_slowdown.mean());
    println!("- mean relative total slowdown = {}", stats.rel_total_slowdown.mean());
}

fn policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    match &s[s.len() - 9..] {
        "keepalive" => {
            let s1 = s.split("-").next().unwrap();
            let len = s1.parse::<f64>().unwrap();
            Box::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0))
        }
        "unloading" => Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0)),
        _ => {
            let mut it = s.split(",");
            it.next();
            let s1 = it.next().unwrap();
            let s2 = s1[1..].split(" ").next().unwrap();
            let len = s2.parse::<f64>().unwrap();
            Box::new(HybridHistogramPolicy::new(3600.0 * len, 60.0, 2.0, 0.5, 0.15, 0.1))
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut trace_config: AzureTraceConfig = Default::default();
    trace_config.invocations_limit = 200000;
    trace_config.concurrency_level = 16;
    let trace = Box::new(process_azure_trace(Path::new(&args[1]), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args[2])).unwrap()).unwrap();
    let policies = experiment_config.coldstart_policies;
    let base_config = experiment_config.base_config;
    let configs: Vec<_> = policies
        .iter()
        .map(|x| {
            let mut config = base_config.clone();
            config.coldstart_policy = x.to_string();
            config
        })
        .collect();
    let mut resolvers: ConfigParamResolvers = Default::default();
    resolvers.coldstart_policy_resolver = Box::new(policy_resolver);
    let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
    for (i, s) in stats.drain(..).enumerate() {
        print_results(s, &policies[i]);
    }
}
