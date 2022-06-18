use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use serverless::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use serverless::function::{Application, Function};
use serverless::resource::{ResourceConsumer, ResourceProvider};
use serverless::simulation::ServerlessSimulation;
use serverless_extra::opendc_trace::process_opendc_trace;
use simcore::simulation::Simulation;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_opendc_trace(Path::new(&args[1]));

    let sim = Simulation::new(1);
    let policy: Option<Rc<RefCell<dyn ColdStartPolicy>>> =
        Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(120.0 * 60.0, 0.0))));
    let mut serverless = ServerlessSimulation::new(sim, None, policy, None);
    for _ in 0..18 {
        let mem = serverless.create_resource("mem", 4096);
        serverless.add_host(None, ResourceProvider::new(vec![mem]));
    }
    for app in trace.iter() {
        let mut max_mem = 0;
        for row in app.iter() {
            max_mem = usize::max(max_mem, row.mem);
        }
        let mem = serverless.create_resource_requirement("mem", max_mem as u64);
        let app_id = serverless.add_app(Application::new(1, 0.5, ResourceConsumer::new(vec![mem])));
        let fn_id = serverless.add_function(Function::new(app_id));
        for row in app.iter() {
            if row.invocations == 0 {
                continue;
            }
            for _ in 0..row.invocations {
                serverless.send_invocation_request(fn_id, (row.exec as f64) / 1000.0, (row.time as f64) / 1000.0);
            }
        }
    }
    serverless.step_until_no_events();
    let stats = serverless.get_stats();
    println!("processed {} invocations", stats.invocations);
}
