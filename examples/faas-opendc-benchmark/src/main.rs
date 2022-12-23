use std::path::Path;
use std::time::Instant;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::Config;
use dslab_faas::extra::opendc_trace::{process_opendc_trace, OpenDCTraceConfig};
use dslab_faas::resource::ResourceProvider;
use dslab_faas::simulation::ServerlessSimulation;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut trace_config: OpenDCTraceConfig = Default::default();
    trace_config.cold_start = 0.5;
    let trace = process_opendc_trace(Path::new(&args[1]), trace_config);
    let mut config: Config = Default::default();
    config.disable_contention = true;
    config.coldstart_policy = Box::new(FixedTimeColdStartPolicy::new(120.0 * 60.0, 0.0));
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    for _ in 0..18 {
        let mem = sim.create_resource("mem", 4096);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 4);
    }
    sim.load_trace(&trace);
    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_secs_f64();
    let stats = sim.get_stats();
    println!(
        "processed {} invocations and {} events in {:.2} seconds ({:.2} events per sec)",
        stats.invocations,
        sim.event_count(),
        elapsed,
        (sim.event_count() as f64) / elapsed
    );
}
