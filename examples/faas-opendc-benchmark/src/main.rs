use std::path::Path;
use std::time::Instant;

use clap::Parser;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::Config;
use dslab_faas::extra::opendc_trace::{process_opendc_trace, OpenDCTraceConfig};
use dslab_faas::resource::ResourceProvider;
use dslab_faas::simulation::ServerlessSimulation;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with OpenDC trace.
    trace: String,
}

fn main() {
    let args = Args::parse();
    let trace_config = OpenDCTraceConfig {
        cold_start: 0.5,
        ..Default::default()
    };
    let trace = process_opendc_trace(Path::new(&args.trace), trace_config);
    let config = Config {
        disable_contention: true,
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(120.0 * 60.0, 0.0)),
        ..Default::default()
    };
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
