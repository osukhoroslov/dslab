use std::io::Write;
use std::path::Path;

use clap::Parser;

use dslab_dag::experiment::Experiment;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// File with configurations
    #[clap(short, long)]
    input: String,

    /// Output file
    #[clap(short, long)]
    output: Option<String>,

    /// Number of threads for running experiment
    #[clap(short, long, default_value = "8")]
    threads: usize,
}
fn main() {
    let args = Args::parse();

    let experiment = Experiment::load(&args.input);

    let result = experiment.run(args.threads);

    std::fs::File::create(args.output.unwrap_or_else(|| {
        let input = Path::new(&args.input);
        input
            .with_file_name([input.file_stem().unwrap().to_str().unwrap(), "-results"].concat())
            .with_extension("json")
            .to_str()
            .unwrap()
            .to_string()
    }))
    .unwrap()
    .write_all(serde_json::to_string_pretty(&result).unwrap().as_bytes())
    .unwrap();
}
