pub mod cmswc;
pub mod kamsa;
pub mod metrics;
pub mod vcaes;
pub mod vmals;

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Duration;

use clap::Parser;
use env_logger::Builder;
use sugars::{rc, refcell};

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::read_network_config;
use dslab_dag::pareto::{ParetoScheduler, ParetoSimulation};
use dslab_dag::pareto_schedulers::moheft::MOHeftScheduler;
use dslab_dag::parsers::config::ParserConfig;
use dslab_dag::resource::read_resource_configs;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::heft::HeftScheduler;

use crate::cmswc::CMSWCScheduler;
use crate::kamsa::KAMSAScheduler;
use crate::metrics::{coverage, hypervolume};
use crate::vcaes::VCAESScheduler;
use crate::vmals::VMALSScheduler;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
/// Runs DSLab DAG benchmark
struct Args {
    /// Path to DAG file in WfCommons-3 format
    #[arg(short, long)]
    dag: String,

    /// Path to system file
    #[arg(short, long)]
    system: String,

    /// Pricing interval
    #[arg(short, long)]
    interval: f64,
}

const N_TRIES: usize = 10;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let args = Args::parse();

    let mut config = ParserConfig::with_reference_speed(100.);
    config.ignore_memory = true;
    let dag = DAG::from_wfcommons(&args.dag, &config);
    let total_tasks = dag.get_tasks().len();

    let mut names = Vec::new();
    let mut fronts = Vec::new();

    let n_schedules = 100;
    let obj_eval_limit = (total_tasks * 200) as i64;
    let mut schedulers: Vec<(String, Rc<RefCell<dyn ParetoScheduler>>)> = Vec::with_capacity(4 * N_TRIES);
    for i in 1..N_TRIES + 1 {
        schedulers.push((
            format!("KAMSA_{}", i),
            rc!(refcell!(KAMSAScheduler::new(
                n_schedules,
                12,
                1e-6,
                20.,
                2.,
                i as u64,
                obj_eval_limit
                //Duration::from_secs_f64(5. * 60.)
            ))),
        ));
    }
    for i in 1..N_TRIES + 1 {
        schedulers.push((
            format!("VCAES_{}", i),
            rc!(refcell!(VCAESScheduler::new(
                n_schedules,
                20,
                2.,
                i as u64,
                obj_eval_limit,
                //Duration::from_secs_f64(5. * 60.)
            ))),
        ));
    }
    for i in 1..N_TRIES + 1 {
        schedulers.push((
                format!("VMALS_{}", i),
                rc!(refcell!(VMALSScheduler::new(
                    n_schedules,
                    2,
                    0.1,
                    0.9,
                    i as u64,
                    obj_eval_limit,
                    //Duration::from_secs_f64(5. * 60.),
                    10,
                    0.3
                ))),
            ));
    }
    for i in 1..N_TRIES + 1 {
        schedulers.push((format!("CMSWC_{}", i), rc!(refcell!(CMSWCScheduler::new(n_schedules, 0.3, i as u64)))));
    }
    for (name, sched) in schedulers.into_iter() {
        print!("running {}...", name);
        let sim = ParetoSimulation::new(
            dag.clone(),
            read_resource_configs(&args.system),
            read_network_config(&args.system),
            sched,
            DataTransferMode::Direct,
            Some(args.interval),
        );
        let mut results = sim.run(n_schedules);
        let metrics = results
            .run_stats
            .iter()
            .map(|x| (x.makespan, x.total_execution_cost))
            .collect::<Vec<_>>();
        names.push(name);
        fronts.push(metrics);
        println!(" finished");
    }
    let max_makespan = fronts
        .iter()
        .map(|f| f.iter().max_by(|a, b| a.0.total_cmp(&b.0)).unwrap().0)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();
    let max_cost = fronts
        .iter()
        .map(|f| f.iter().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap().1)
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();
    println!("Hypervolumes:");
    for (i, name) in names.iter().enumerate() {
        println!(
            "{} hv = {:.5}",
            name,
            hypervolume(fronts[i].clone(), (max_makespan, max_cost))
        );
    }
    println!("");
    let n_algos = names.len();
    assert_eq!(fronts.len(), n_algos);
    let max_len = names.iter().map(|x| x.len()).max().unwrap();
    println!("C-metric table:");
    print!("{: <1$}", "", max_len);
    for name in &names {
        print!("{: >1$}", name, max_len + 1);
    }
    println!("");
    for (i, front_i) in fronts.iter().enumerate() {
        print!("{: >1$}", &names[i], max_len);
        for front_j in fronts.iter() {
            print!("{: >1$.3}", coverage(front_i, front_j), max_len + 1);
        }
        println!("");
    }
}
