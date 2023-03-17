use std::io::Write;
use std::path::PathBuf;

use clap::Parser;

use dslab_dag::experiment::Experiment;
use dslab_dag::scheduler::default_scheduler_resolver;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// Runs batch experiment with DSLab DAG
struct Args {
    /// Path to YAML file with experiment configuration
    #[arg(short, long)]
    config: PathBuf,

    /// Path to produced JSON file with experiment results
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of threads to use (default - use all available cores)
    #[arg(short, long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let experiment = Experiment::load(&args.config, default_scheduler_resolver);

    let results = experiment.run(args.threads);

    std::fs::File::create(args.output.unwrap_or_else(|| {
        args.config
            .with_file_name([args.config.file_stem().unwrap().to_str().unwrap(), "-results"].concat())
            .with_extension("json")
    }))?
    .write_all(serde_json::to_string_pretty(&results).unwrap().as_bytes())
}
