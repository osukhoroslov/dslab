use std::boxed::Box;
use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};

use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::parallel::parallel_simulation_raw;
use dslab_faas::stats::Stats;
use dslab_faas_extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas_extra::resolvers::{extra_coldstart_policy_resolver, extra_scheduler_resolver};

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub schedulers: Vec<String>,
}

fn print_results(stats: Stats, name: &str) {
    println!("describing {}", name);
    println!("- {} successful invocations", stats.invocations);
    println!(
        "- cold start rate = {}",
        (stats.cold_starts as f64) / (stats.invocations as f64)
    );
    println!(
        "- wasted memory time = {}",
        stats.wasted_resource_time.get(&0).unwrap().sum()
    );
    println!(
        "- mean absolute execution slowdown = {}",
        stats.abs_exec_slowdown.mean()
    );
    println!(
        "- mean relative execution slowdown = {}",
        stats.rel_exec_slowdown.mean()
    );
    println!("- mean absolute total slowdown = {}", stats.abs_total_slowdown.mean());
    println!("- mean relative total slowdown = {}", stats.rel_total_slowdown.mean());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut trace_config: AzureTraceConfig = Default::default();
    trace_config.invocations_limit = 200000;
    let trace = Box::new(process_azure_trace(Path::new(&args[1]), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args[2])).unwrap()).unwrap();
    let schedulers = experiment_config.schedulers;
    let sim_config = experiment_config.base_config;
    let configs: Vec<_> = schedulers
        .iter()
        .map(|x| {
            let mut config = sim_config.clone();
            config.scheduler = x.to_string();
            config
        })
        .collect();
    let mut resolvers: ConfigParamResolvers = Default::default();
    resolvers.coldstart_policy_resolver = Box::new(extra_coldstart_policy_resolver);
    resolvers.scheduler_resolver = Box::new(extra_scheduler_resolver);
    let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
    for (i, s) in stats.drain(..).enumerate() {
        print_results(s, &schedulers[i]);
    }
}
