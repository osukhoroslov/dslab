use std::boxed::Box;
use std::fs::File;
use std::path::Path;

use clap::Parser;

use serde::{Deserialize, Serialize};

use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas::extra::resolvers::{extra_coldstart_policy_resolver, extra_scheduler_resolver};
use dslab_faas::parallel::parallel_simulation_raw;
use dslab_faas::stats::GlobalStats;

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub schedulers: Vec<String>,
}

fn print_results(stats: GlobalStats, name: &str) {
    println!("describing {}", name);
    println!("- {} successful invocations", stats.invocation_stats.invocations);
    println!(
        "- cold start rate = {}",
        (stats.invocation_stats.cold_starts as f64) / (stats.invocation_stats.invocations as f64)
    );
    println!("- wasted memory time = {}", stats.wasted_resource_time[&0].sum());
    println!(
        "- 99% absolute execution slowdown = {}",
        stats.invocation_stats.abs_exec_slowdown.quantile(0.99)
    );
    println!(
        "- 99% relative execution slowdown = {}",
        stats.invocation_stats.rel_exec_slowdown.quantile(0.99)
    );
    println!(
        "- 99% absolute total slowdown = {}",
        stats.invocation_stats.abs_total_slowdown.quantile(0.99)
    );
    println!(
        "- 99% relative total slowdown = {}",
        stats.invocation_stats.rel_total_slowdown.quantile(0.99)
    );
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with Azure Functions trace.
    trace: String,
    /// Path to a simulation config in YAML format.
    #[arg(long)]
    config: String,
}

fn main() {
    let args = Args::parse();
    let trace_config = AzureTraceConfig {
        invocations_limit: 100000,
        force_fixed_memory: Some(256),
        ..Default::default()
    };
    let trace = Box::new(process_azure_trace(Path::new(&args.trace), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args.config)).unwrap()).unwrap();
    let schedulers = experiment_config.schedulers;
    let base_config = experiment_config.base_config;
    let configs: Vec<_> = schedulers
        .iter()
        .map(|x| {
            let mut config = base_config.clone();
            config.scheduler = x.to_string();
            config
        })
        .collect();
    let resolvers = ConfigParamResolvers {
        coldstart_policy_resolver: Box::new(extra_coldstart_policy_resolver),
        scheduler_resolver: Box::new(extra_scheduler_resolver),
        ..Default::default()
    };
    let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
    for (i, s) in stats.drain(..).enumerate() {
        print_results(s.global_stats, &schedulers[i]);
    }
}
