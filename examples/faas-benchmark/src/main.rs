use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use itertools::Itertools;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::FixedTimeColdStartPolicy;
use dslab_faas::config::Config;
use dslab_faas::cpu::IgnoredCpuPolicy;
use dslab_faas::extra::azure_trace_2019::{
    process_azure_2019_trace, AppPreference, Azure2019TraceConfig, StartGenerator,
};
use dslab_faas::resource::ResourceProvider;
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::trace::Trace;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Preset {
    Skewed,
    Balanced,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with Azure trace.
    trace: String,
    /// Dump generated trace in OpenDC format.
    #[arg(long)]
    dump: Option<String>,
    /// Don't run the benchmark.
    #[arg(long)]
    norun: bool,
    /// Benchmark preset: skewed or balanced.
    #[arg(long, value_enum)]
    preset: Preset,
    /// Benchmark load (requests per second).
    #[arg(long)]
    rps: f64,
}

/// Dumps trace in OpenDC format.
fn dump_trace(trace: &dyn Trace, path: &Path) {
    assert!(trace.is_ordered_by_time());
    create_dir_all(path).unwrap();
    let func_id = trace.function_iter().collect::<Vec<_>>();
    let app_mem = trace.app_iter().map(|x| x.container_resources[0].1).collect::<Vec<_>>();
    let mut app_files = (0..app_mem.len())
        .map(|i| BufWriter::new(File::create(path.join(format!("{}.csv", i))).unwrap()))
        .collect::<Vec<_>>();
    for file in &mut app_files {
        writeln!(file, "Timestamp [ms],Invocations, Avg Exec time per Invocation,Provisioned CPU [Mhz],Provisioned Memory [mb], Avg cpu usage per Invocation [Mhz], Avg mem usage per Invocation [mb]").unwrap();
    }
    for (timestamp, group) in &trace.request_iter().group_by(|r| (r.time * 1000.0).round() as u64) {
        let mut cnt = vec![0; app_mem.len()];
        let mut sum_dur = vec![0f64; app_mem.len()];
        for req in group {
            let app = func_id[req.id];
            cnt[app] += 1;
            sum_dur[app] += req.duration;
        }
        for app in 0..cnt.len() {
            let mean = if cnt[app] > 0 {
                sum_dur[app] / (cnt[app] as f64)
            } else {
                0f64
            };
            writeln!(
                app_files[app],
                "{},{},{},100,{},0,{}",
                timestamp,
                cnt[app],
                (mean * 1000.0).round() as u64,
                app_mem[app],
                app_mem[app]
            )
            .unwrap();
        }
    }
    for file in &mut app_files {
        file.flush().unwrap();
    }
}

fn main() {
    let args = Args::parse();
    let prefs = match args.preset {
        Preset::Skewed => vec![AppPreference::new(1, 0.0, 0.1), AppPreference::new(29, 0.4, 0.6)],
        Preset::Balanced => vec![AppPreference::new(30, 0.48, 0.52)],
    };
    let trace_config = Azure2019TraceConfig {
        time_period: 24 * 60,
        cold_start_latency: 0.5,
        app_preferences: prefs,
        start_generator: StartGenerator::PoissonFit,
        rps: Some(args.rps),
        ..Default::default()
    };
    let trace = process_azure_2019_trace(Path::new(&args.trace), trace_config);
    println!("Trace generated successfully!");
    if let Some(path) = args.dump {
        dump_trace(&trace, Path::new(&path));
        println!("Trace dumped to {}", path);
    }
    if args.norun {
        return;
    }
    let config = Config {
        cpu_policy: Box::<IgnoredCpuPolicy>::default(),
        coldstart_policy: Box::new(FixedTimeColdStartPolicy::new(10.0 * 60.0, 0.0, false)),
        ..Default::default()
    };
    let mut sim = ServerlessSimulation::new(Simulation::new(1), config);
    for _ in 0..100 {
        let mem = sim.create_resource("mem", 4096);
        sim.add_host(None, ResourceProvider::new(vec![mem]), 4);
    }
    sim.load_trace(&trace);
    let t = Instant::now();
    sim.step_until_no_events();
    let elapsed = t.elapsed().as_secs_f64();
    let stats = sim.invocation_stats();
    println!(
        "processed {} invocations (rps = {:.3}) and {} events in {:.2} seconds ({:.2} events per sec)",
        stats.invocations,
        args.rps,
        sim.event_count(),
        elapsed,
        (sim.event_count() as f64) / elapsed
    );
}
