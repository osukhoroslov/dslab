use std::path::Path;

use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::parallel::{parallel_simulation, ParallelConfig, ParallelHostConfig};
use dslab_faas::stats::Stats;
use dslab_faas_extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas_extra::hybrid_histogram::HybridHistogramPolicy;

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
    let mut policies: Vec<Box<dyn ColdStartPolicy + Send>> = Vec::new();
    let mut descr = Vec::new();
    policies.push(Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0)));
    descr.push("No unloading".to_string());
    for len in &[20.0, 45.0, 60.0, 90.0, 120.0] {
        policies.push(Box::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0)));
        descr.push(format!("{}-minute keepalive", len));
    }
    for len in &[2.0, 3.0, 4.0] {
        policies.push(Box::new(HybridHistogramPolicy::new(
            3600.0 * len,
            60.0,
            2.0,
            0.5,
            0.15,
            0.1,
        )));
        descr.push(format!("Hybrid Histogram policy, {} hours bound", len));
    }
    let configs: Vec<_> = policies
        .drain(..)
        .map(|x| {
            let mut config: ParallelConfig = Default::default();
            config.coldstart_policy = x;
            for _ in 0..1000 {
                let mut host: ParallelHostConfig = Default::default();
                host.resources = vec![("mem".to_string(), 4096 * 4)];
                host.cores = 8;
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
