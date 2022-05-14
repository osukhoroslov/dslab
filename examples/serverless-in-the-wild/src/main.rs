use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::function::{Application, Function};
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::stats::Stats;
use dslab_faas_extra::azure_experiment::{process_azure_trace, Trace};
use dslab_faas_extra::hybrid_histogram::HybridHistogramPolicy;

fn test_policy(policy: Option<Rc<RefCell<dyn ColdStartPolicy>>>, trace: &Trace) -> Stats {
    let mut time_range = 0.0;
    for req in trace.trace_records.iter() {
        time_range = f64::max(time_range, req.time + req.dur);
    }
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, None, policy, None);
    for _ in 0..1000 {
        let mem = serverless.create_resource("mem", 4096 * 4);
        serverless.add_host(None, ResourceProvider::new(vec![mem]));
    }
    for app in trace.app_records.iter() {
        let mem = serverless.create_resource_requirement("mem", app.mem);
        serverless.add_app(Application::new(16, app.cold_start, ResourceConsumer::new(vec![mem])));
    }
    for func in trace.function_records.iter() {
        serverless.add_function(Function::new(func.app_id));
    }
    for req in trace.trace_records.iter() {
        serverless.send_invocation_request(req.id as u64, req.dur, req.time);
    }
    serverless.set_simulation_end(time_range);
    serverless.step_until_no_events();
    serverless.get_stats()
}

fn print_results(stats: Stats, name: &str) {
    println!("describing {}", name);
    println!("{} successful invocations", stats.invocations);
    println!(
        "cold start rate = {}",
        (stats.cold_starts as f64) / (stats.invocations as f64)
    );
    println!("wasted memory time = {}", *stats.wasted_resource_time.get(&0).unwrap());
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
            Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(
                f64::MAX / 10.0,
                0.0,
            )))),
            &trace,
        ),
        "No unloading",
    );
    for len in vec![20.0, 45.0, 60.0, 90.0, 120.0] {
        print_results(
            test_policy(
                Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0)))),
                &trace,
            ),
            &format!("{}-minute keepalive", len),
        );
    }
    for len in vec![2.0, 3.0, 4.0] {
        print_results(
            test_policy(
                Some(Rc::new(RefCell::new(HybridHistogramPolicy::new(
                    3600.0 * len,
                    60.0,
                    2.0,
                    0.5,
                    0.15,
                    0.1,
                )))),
                &trace,
            ),
            &format!("Hybrid Histogram policy, {} hours bound", len),
        );
    }
}
