use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::config::Config;
use dslab_faas::function::{Application, Function};
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::stats::Stats;
use dslab_faas_extra::azure_trace::{process_azure_trace, Trace};
use dslab_faas_extra::hybrid_histogram::HybridHistogramPolicy;

fn test_policy(policy: Rc<RefCell<dyn ColdStartPolicy>>, trace: &Trace) -> Stats {
    let mut time_range = 0.0;
    for req in trace.trace_records.iter() {
        time_range = f64::max(time_range, req.time + req.dur);
    }
    let mut config: Config = Default::default();
    config.coldstart_policy = policy;
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    for _ in 0..1000 {
        let mem = sim.create_resource("mem", 4096 * 4);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 8);
    }
    for app in trace.app_records.iter() {
        let mem = sim.create_resource_requirement("mem", app.mem);
        sim.add_app(Application::new(
            16,
            app.cold_start,
            1.0,
            ResourceConsumer::new(vec![mem]),
        ));
    }
    for func in trace.function_records.iter() {
        sim.add_function(Function::new(func.app_id));
    }
    for req in trace.trace_records.iter() {
        sim.send_invocation_request(req.id as u64, req.dur, req.time);
    }
    sim.set_simulation_end(time_range);
    sim.step_until_no_events();
    sim.get_stats()
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_azure_trace(Path::new(&args[1]), 200000);
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    print_results(
        test_policy(
            Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0))),
            &trace,
        ),
        "No unloading",
    );
    for len in vec![20.0, 45.0, 60.0, 90.0, 120.0] {
        print_results(
            test_policy(
                Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0))),
                &trace,
            ),
            &format!("{}-minute keepalive", len),
        );
    }
    for len in vec![2.0, 3.0, 4.0] {
        print_results(
            test_policy(
                Rc::new(RefCell::new(HybridHistogramPolicy::new(
                    3600.0 * len,
                    60.0,
                    2.0,
                    0.5,
                    0.15,
                    0.1,
                ))),
                &trace,
            ),
            &format!("Hybrid Histogram policy, {} hours bound", len),
        );
    }
}
