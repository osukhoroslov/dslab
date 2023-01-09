use std::io::Write;
use std::path::Path;

use clap::Parser;

use dslab_dag::experiment::Experiment;
use dslab_dag::scheduler_resolver::default_scheduler_resolver;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to file with experiment configuration
    #[clap(short, long)]
    config: String,

    /// Path to output file with experiment results
    #[clap(short, long)]
    output: Option<String>,

    /// Number of threads for running experiment
    #[clap(short, long, default_value_t = std::thread::available_parallelism().unwrap().get())]
    threads: usize,
}

fn main() {
    let args = Args::parse();

    let experiment = Experiment::load(&args.config, default_scheduler_resolver);

    let result = experiment.run(args.threads);

    std::fs::File::create(args.output.unwrap_or_else(|| {
        let config = Path::new(&args.config);
        config
            .with_file_name([config.file_stem().unwrap().to_str().unwrap(), "-results"].concat())
            .with_extension("json")
            .to_str()
            .unwrap()
            .to_string()
    }))
    .unwrap()
    .write_all(serde_json::to_string_pretty(&result).unwrap().as_bytes())
    .unwrap();
}
