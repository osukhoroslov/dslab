use std::path::Path;

use clap::Parser;

use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas::parallel::{parallel_simulation, ParallelConfig, ParallelHostConfig};
use dslab_faas::stats::Stats;

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

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap()]
    trace: String,
}

fn main() {
    let args = Args::parse();
    let trace_config = AzureTraceConfig {
        invocations_limit: 20000,
        ..Default::default()
    };
    let trace = Box::new(process_azure_trace(Path::new(&args.trace), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let mut policies: Vec<Box<dyn ColdStartPolicy + Send>> = Vec::new();
    let mut descr = Vec::new();
    for len in &[5.0, 10.0, 20.0, 45.0, 60.0, 90.0, 120.0, 150.0, 180.0] {
        policies.push(Box::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0)));
        descr.push(format!("{}-minute keepalive", len));
    }
    let configs: Vec<_> = policies
        .drain(..)
        .map(|x| {
            let mut config = ParallelConfig {
                coldstart_policy: x,
                ..Default::default()
            };
            for _ in 0..18 {
                let host = ParallelHostConfig {
                    resources: vec![("mem".to_string(), 4096 * 4)],
                    cores: 8,
                    ..Default::default()
                };
                config.hosts.push(host);
            }
            config
        })
        .collect();
    let mut stats = parallel_simulation(configs, vec![trace], vec![1]);
    for (i, s) in stats.drain(..).enumerate() {
        print_results(s, &descr[i]);
    }
}
