use std::boxed::Box;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use serverless::coldstart::FixedTimeColdStartPolicy;
use serverless::function::{Application, Function};
use serverless::resource::{ResourceConsumer, ResourceProvider};
use serverless::scheduler::Scheduler;
use serverless::simulation::ServerlessSimulation;
use serverless::stats::Stats;
use serverless_extra::azure_experiment::{process_azure_trace, Trace};
use serverless_extra::hermes::HermesScheduler;
use serverless_extra::simple_schedulers::*;
use simcore::simulation::Simulation;

fn test_scheduler(scheduler: Option<Box<dyn Scheduler>>, trace: &Trace) -> Stats {
    let mut time_range = 0.0;
    for req in trace.trace_records.iter() {
        time_range = f64::max(time_range, req.time + req.dur);
    }
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(
        sim,
        None,
        Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, 0.0)))),
        scheduler,
    );
    for _ in 0..10 {
        let mem = serverless.create_resource("mem", 4096 * 4);
        serverless.add_host(None, ResourceProvider::new(vec![mem]), 4, None);
    }
    for app in trace.app_records.iter() {
        let mem = serverless.create_resource_requirement("mem", app.mem);
        serverless.add_app(Application::new(
            1,
            app.cold_start,
            1.,
            ResourceConsumer::new(vec![mem]),
        ));
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
    println!("mean absolute slowdown = {}", stats.abs_slowdown.mean());
    println!("mean relative slowdown = {}", stats.rel_slowdown.mean());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_azure_trace(Path::new(&args[1]), 200000);
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    print_results(
        test_scheduler(Some(Box::new(LocalityBasedScheduler::new(None, None, true))), &trace),
        "Locality-based, warm only",
    );
    print_results(
        test_scheduler(Some(Box::new(LocalityBasedScheduler::new(None, None, false))), &trace),
        "Locality-based, allow cold",
    );
    print_results(
        test_scheduler(Some(Box::new(RandomScheduler::new(1))), &trace),
        "Random",
    );
    print_results(
        test_scheduler(Some(Box::new(LeastLoadedScheduler::new(true))), &trace),
        "Least-loaded",
    );
    print_results(
        test_scheduler(Some(Box::new(RoundRobinScheduler::new())), &trace),
        "Round Robin",
    );
    print_results(test_scheduler(Some(Box::new(HermesScheduler::new())), &trace), "Hermes");
}
