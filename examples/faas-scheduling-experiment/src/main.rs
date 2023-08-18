mod plot;

use std::boxed::Box;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use clap::{Parser, ValueEnum};

use serde::{Deserialize, Serialize};

use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::extra::azure_trace_2019::{
    process_azure_2019_trace, AppPreference, Azure2019TraceConfig, DurationGenerator, StartGenerator,
};
use dslab_faas::extra::resolvers::{extra_coldstart_policy_resolver, extra_scheduler_resolver};
use dslab_faas::parallel::parallel_simulation_raw;

use crate::plot::plot_results;

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub schedulers: Vec<String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Preset {
    Skewed,
    Balanced,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with Azure Functions trace.
    trace: String,
    /// Path to a simulation config in YAML format.
    #[arg(long)]
    config: String,
    /// Workload preset.
    #[arg(long, value_enum)]
    preset: Preset,
    /// Plot output path.
    #[arg(long)]
    plot: String,
    /// Dump final metrics to given file.
    #[arg(long)]
    dump: Option<String>,
}

fn main() {
    let args = Args::parse();
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args.config)).unwrap()).unwrap();
    let schedulers = experiment_config.schedulers;
    let base_config = experiment_config.base_config;
    let rps_vec = (1..15).map(|x| x as f64).collect::<Vec<f64>>();
    let mut points = vec![Vec::with_capacity(rps_vec.len()); schedulers.len()];
    let prefs = match args.preset {
        Preset::Skewed => vec![AppPreference::new(1, 0.02, 0.05), AppPreference::new(49, 0.45, 0.55)],
        Preset::Balanced => vec![AppPreference::new(50, 0.45, 0.55)],
    };
    for rps in rps_vec.iter() {
        let trace_config = Azure2019TraceConfig {
            time_period: 60,
            duration_generator: DurationGenerator::PrefittedLognormal,
            start_generator: StartGenerator::PoissonFit,
            app_preferences: prefs.clone(),
            force_fixed_memory: Some(256),
            rps: Some(*rps),
            ..Default::default()
        };
        let trace = Box::new(process_azure_2019_trace(Path::new(&args.trace), trace_config));
        println!(
            "trace processed successfully, got {} invocations at {} RPS",
            trace.trace_records.len(),
            *rps
        );
        let configs: Vec<_> = schedulers
            .iter()
            .map(|x| {
                let mut config = base_config.clone();
                config.scheduler = x.to_string();
                config
            })
            .collect();
        let resolvers = ConfigParamResolvers {
            coldstart_policy_resolver: Box::new(extra_coldstart_policy_resolver),
            scheduler_resolver: Box::new(extra_scheduler_resolver),
            ..Default::default()
        };
        let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
        for (i, s) in stats.drain(..).enumerate() {
            let inv = s.global_stats.invocation_stats;
            points[i].push([
                inv.rel_total_slowdown.quantile(0.99) + 1.,
                (inv.cold_starts as f64) / (inv.invocations as f64) * 100.,
            ]);
        }
    }
    if let Some(s) = args.dump {
        let mut out = File::create(s).unwrap();
        writeln!(&mut out, "scheduler,rps,99% slowdown,cold start %").unwrap();
        for (sched, pts) in std::iter::zip(schedulers.iter(), points.iter()) {
            for (rps, pt) in std::iter::zip(rps_vec.iter(), pts.iter()) {
                writeln!(&mut out, "{},{},{:.4},{:.4}", sched, rps, pt[0], pt[1]).unwrap();
            }
        }
    }
    plot_results(&args.plot, &schedulers, &rps_vec, &points);
}
