mod plot;

use std::boxed::Box;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use clap::Parser;

use serde::{Deserialize, Serialize};

use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::extra::azure_trace_2019::{process_azure_2019_trace, AppPreference, Azure2019TraceConfig};
use dslab_faas::extra::hybrid_histogram::HybridHistogramPolicy;
use dslab_faas::parallel::parallel_simulation_raw;
use dslab_faas::stats::SampleMetric;

use crate::plot::{plot_cdf, plot_metrics};

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub coldstart_policies: Vec<String>,
}

fn policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    match &s[s.len() - 9..] {
        "keepalive" => {
            let s1 = s.split('-').next().unwrap();
            let len = s1.parse::<f64>().unwrap();
            Box::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0, false))
        }
        "unloading" => Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0, true)),
        _ => {
            let mut it = s.split(',');
            it.next();
            let s1 = it.next().unwrap();
            let s2 = s1[1..].split(' ').next().unwrap();
            let len = s2.parse::<f64>().unwrap();
            Box::new(HybridHistogramPolicy::new(3600.0 * len, 60.0, 2.0, 0.5, 0.15, 0.1))
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with Azure Functions trace.
    trace: String,
    /// Path to a simulation config in YAML format.
    #[arg(long)]
    config: String,
    /// Metrics plot output path (if needed).
    #[arg(long)]
    plot_metrics: Option<String>,
    /// App coldstart CDF plot output path (if needed).
    #[arg(long)]
    plot_cdf: Option<String>,
    /// Dump final metrics to given file.
    #[arg(long)]
    dump: Option<String>,
}

fn main() {
    let args = Args::parse();
    let trace_config = Azure2019TraceConfig {
        time_period: 8 * 60,
        app_preferences: vec![
            AppPreference::new(17, 0.3, 0.4),
            AppPreference::new(17, 0.4, 0.5),
            AppPreference::new(17, 0.5, 0.6),
            AppPreference::new(17, 0.6, 0.7),
        ],
        ..Default::default()
    };
    let trace = Box::new(process_azure_2019_trace(Path::new(&args.trace), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args.config)).unwrap()).unwrap();
    let policies = experiment_config.coldstart_policies;
    let base_config = experiment_config.base_config;
    let configs: Vec<_> = policies
        .iter()
        .map(|x| {
            let mut config = base_config.clone();
            config.coldstart_policy = x.to_string();
            config
        })
        .collect();
    let resolvers = ConfigParamResolvers {
        coldstart_policy_resolver: Box::new(policy_resolver),
        ..Default::default()
    };
    let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
    let mut results = Vec::with_capacity(stats.len());
    let mut full_results = Vec::with_capacity(stats.len());
    for (i, s) in stats.drain(..).enumerate() {
        s.global_stats.print_summary(&policies[i]);
        if args.plot_metrics.is_some() || args.plot_cdf.is_some() {
            let mut apps: SampleMetric = Default::default();
            for app_stats in s.app_stats.iter() {
                apps.add((app_stats.cold_starts as f64) / (app_stats.invocations as f64) * 100.);
            }
            full_results.push(apps.to_vec());
            results.push((
                apps.quantile(0.75),
                s.global_stats.wasted_resource_time.get(0).unwrap().sum(),
            ));
        }
    }
    if let Some(s) = args.dump {
        let mut out = File::create(s).unwrap();
        writeln!(&mut out, "policy,75% app coldstart frequency,wasted memory time").unwrap();
        for (policy, result) in std::iter::zip(policies.iter(), results.iter()) {
            writeln!(&mut out, "{},{:.4},{:.4}", policy, result.0, result.1).unwrap();
        }
    }
    if let Some(plot) = args.plot_metrics {
        let mut pos = usize::MAX;
        for (i, p) in policies.iter().enumerate() {
            if p.contains("10-minute keepalive") {
                pos = i;
                break;
            }
        }
        assert!(pos != usize::MAX);
        let base = results[pos].1;
        for p in results.iter_mut() {
            p.1 = p.1 / base * 100.;
        }
        plot_metrics(&plot, policies.clone(), results);
    }
    if let Some(plot) = args.plot_cdf {
        plot_cdf(&plot, policies, full_results);
    }
}
