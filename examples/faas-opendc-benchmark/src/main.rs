use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::function::{Application, Function};
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas_extra::opendc_trace::process_opendc_trace;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_opendc_trace(Path::new(&args[1]));

    let policy: Option<Rc<RefCell<dyn ColdStartPolicy>>> =
        Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(120.0 * 60.0, 0.0))));
    let mut sim = ServerlessSimulation::new(Simulation::new(1), None, policy, None);
    for _ in 0..18 {
        let mem = sim.create_resource("mem", 4096);
        sim.add_host(None, ResourceProvider::new(vec![mem]));
    }
    for app in trace.iter() {
        let mut max_mem = 0;
        for sample in app.iter() {
            max_mem = usize::max(max_mem, sample.mem_provisioned);
        }
        let mem = sim.create_resource_requirement("mem", max_mem as u64);
        let app_id = sim.add_app(Application::new(1, 0.5, ResourceConsumer::new(vec![mem])));
        let fn_id = sim.add_function(Function::new(app_id));
        for sample in app.iter() {
            if sample.invocations == 0 {
                continue;
            }
            for _ in 0..sample.invocations {
                sim.send_invocation_request(fn_id, (sample.exec as f64) / 1000.0, (sample.time as f64) / 1000.0);
            }
        }
    }
    sim.step_until_no_events();
    let stats = sim.get_stats();
    println!("processed {} invocations", stats.invocations);
}
