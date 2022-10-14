use std::boxed::Box;
use std::path::Path;

use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::parallel::{parallel_simulation, ParallelConfig, ParallelHostConfig};
use dslab_faas::scheduler::Scheduler;
use dslab_faas::stats::Stats;
use dslab_faas_extra::azure_trace::{process_azure_trace, AzureTraceConfig};
use dslab_faas_extra::hermes::HermesScheduler;
use dslab_faas_extra::simple_schedulers::*;

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
    let mut schedulers: Vec<Box<dyn Scheduler + Send>> = vec![
        Box::new(LocalityBasedScheduler::new(None, None, true)),
        Box::new(LocalityBasedScheduler::new(None, None, false)),
        Box::new(RandomScheduler::new(1)),
        Box::new(LeastLoadedScheduler::new(true)),
        Box::new(RoundRobinScheduler::new()),
        Box::new(HermesScheduler::new()),
    ];
    let descr = schedulers.iter().map(|x| x.get_name()).collect::<Vec<_>>();
    let configs: Vec<_> = schedulers
        .drain(..)
        .map(|x| {
            let mut config: ParallelConfig = Default::default();
            config.scheduler = x;
            config.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0));
            for _ in 0..10 {
                let mut host: ParallelHostConfig = Default::default();
                host.resources = vec![("mem".to_string(), 4096 * 4)];
                host.cores = 4;
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
