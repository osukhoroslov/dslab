use core::simulation::Simulation;

use csv::ReaderBuilder;

use rand::prelude::*;
use rand_pcg::Pcg64;

use serverless::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use serverless::function::Function;
use serverless::invoker::InvocationRequest;
use serverless::resource::{Resource, ResourceConsumer, ResourceProvider, ResourceRequirement};
use serverless::simulation::ServerlessSimulation;
use serverless::stats::Stats;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::{read_dir, File};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;

#[derive(Default, Clone, Copy)]
struct TraceRecord {
    pub id: usize,
    pub time: f64,
    pub dur: f64,
}

#[derive(Default, Clone, Copy)]
struct FunctionRecord {
    pub mem: u64,
    pub cold_start: f64,
}

type Trace = (Vec<TraceRecord>, Vec<FunctionRecord>);

fn gen_sample<T: Copy>(gen: &mut Pcg64, perc: &Vec<f64>, vals: &Vec<T>) -> T {
    let p = gen.gen_range(0.0..1.0);
    if p < perc[0] {
        return vals[0];
    }
    for i in 1..perc.len() {
        if p < perc[i] && p >= perc[i - 1] {
            return vals[i];
        }
    }
    vals[vals.len() - 1]
}

fn process_azure_trace(path: &Path, invocations_limit: usize) -> Trace {
    let mut gen = Pcg64::seed_from_u64(1);
    let mut trace = Vec::new();
    let mut parts = HashSet::<String>::new();
    let mut mem = HashMap::<String, PathBuf>::new();
    let mut inv = HashMap::<String, PathBuf>::new();
    let mut dur = HashMap::<String, PathBuf>::new();
    match read_dir(path) {
        Ok(paths) => {
            for entry_ in paths {
                let entry = entry_.unwrap();
                let path = entry.path();
                if !path.as_path().is_file() {
                    continue;
                }
                let st = path.as_path().file_stem().unwrap().to_str().unwrap();
                let part = st.rsplit('.').next().unwrap().to_string();
                parts.insert(part.clone());
                if st.starts_with("app") {
                    mem.insert(part, path);
                } else if st.starts_with("invocations") {
                    inv.insert(part, path);
                } else if st.starts_with("function") {
                    dur.insert(part, path);
                } else {
                    panic!("bad filename: {}", entry.file_name().to_str().unwrap());
                }
            }
        }
        Err(e) => {
            panic!("error while reading trace dir: {}", e);
        }
    }
    let mut fn_data = HashMap::<String, (usize, f64, u64)>::new();
    let dur_percent = vec![0., 0.01, 0.25, 0.50, 0.75, 0.99, 1.];
    let mem_percent = vec![0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1.];
    let limit = invocations_limit / parts.len();
    for part in parts.iter() {
        let mut now = 0;
        let mut mem_file = ReaderBuilder::new()
            .from_path(mem.get(part).unwrap().as_path())
            .unwrap();
        let mut inv_file = ReaderBuilder::new()
            .from_path(inv.get(part).unwrap().as_path())
            .unwrap();
        let mut dur_file = ReaderBuilder::new()
            .from_path(dur.get(part).unwrap().as_path())
            .unwrap();
        let mut dur_dist = HashMap::<String, Vec<f64>>::new();
        for dur_rec in dur_file.records() {
            let record = dur_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            id.push('_');
            id.push_str(&record[2]);
            let mut perc = Vec::with_capacity(dur_percent.len());
            for i in 7..record.len() {
                let val = f64::from_str(&record[i]).unwrap();
                perc.push(val);
            }
            dur_dist.insert(id, perc);
        }
        let mut inv_cnt = HashMap::<String, Vec<usize>>::new();
        for inv_rec in inv_file.records() {
            let record = inv_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            id.push('_');
            id.push_str(&record[2]);
            let mut cnt = Vec::with_capacity(1440);
            for i in 0..1440 {
                cnt.push(usize::from_str(&record[4 + i]).unwrap());
            }
            inv_cnt.insert(id, cnt);
        }
        let mut mem_dist = HashMap::<String, Vec<usize>>::new();
        for mem_rec in mem_file.records() {
            let record = mem_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            let mut perc = Vec::with_capacity(mem_percent.len());
            for i in 4..record.len() {
                let val = usize::from_str(&record[i]).unwrap();
                perc.push(val);
            }
            mem_dist.insert(id, perc);
        }
        let day = usize::from_str(&part[1..]).unwrap() - 1;
        for (id, dur_vec) in dur_dist.iter() {
            if now == limit {
                break;
            }
            if let Some(inv_vec) = inv_cnt.get(id) {
                let mut id0 = String::new();
                let mut und = false;
                for c in id.chars() {
                    if c == '_' {
                        if und {
                            break;
                        } else {
                            id0.push(c);
                            und = true;
                        }
                    } else {
                        id0.push(c);
                    }
                }
                if let Some(mem_vec) = mem_dist.get(&id0) {
                    if !fn_data.contains_key(id) {
                        let idx = fn_data.len();
                        let mem = gen_sample(&mut gen, &mem_percent, mem_vec);
                        fn_data.insert(id.clone(), (idx, 10.0, mem as u64));
                    }
                    let idx = fn_data.get(id).unwrap().0;
                    for i in 0..1440 {
                        for inv in 0..inv_vec[i] {
                            let second = gen.gen_range(0.0..1.0) * 60.0 + ((i * 60 + day * 1440 * 64) as f64);
                            let record = TraceRecord {
                                id: idx,
                                time: second,
                                dur: gen_sample(&mut gen, &dur_percent, dur_vec),
                            };
                            now += 1;
                            trace.push(record);
                            if now == limit {
                                break;
                            }
                        }
                        if now == limit {
                            break;
                        }
                    }
                }
            }
        }
    }
    let mut funcs: Vec<FunctionRecord> = vec![Default::default(); fn_data.len()];
    for (_, data) in fn_data.iter() {
        funcs[data.0] = FunctionRecord {
            mem: data.2,
            cold_start: data.1,
        }
    }
    trace.sort_by(|x: &TraceRecord, y: &TraceRecord| x.time.partial_cmp(&y.time).unwrap());
    (trace, funcs)
}

fn test_policy(policy: Option<Rc<RefCell<dyn ColdStartPolicy>>>, trace: &Trace) -> Stats {
    let mut time_range = 0.0;
    for req in trace.0.iter() {
        time_range = f64::max(time_range, req.time + req.dur);
    }
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, None, None, policy);
    for i in 0..600 {
        serverless.new_host(ResourceProvider::new(HashMap::<String, Resource>::from([(
            "mem".to_string(),
            Resource::new("mem".to_string(), 4096),
        )])));
    }
    for func in trace.1.iter() {
        serverless.new_function(Function::new(
            func.cold_start,
            ResourceConsumer::new(HashMap::<String, ResourceRequirement>::from([(
                "mem".to_string(),
                ResourceRequirement::new("mem".to_string(), func.mem),
            )])),
        ));
    }
    for req in trace.0.iter() {
        serverless.send_invocation_request(
            req.time,
            InvocationRequest {
                id: req.id as u64,
                duration: req.dur,
            },
        );
    }
    serverless.set_simulation_end(time_range);
    serverless.step_until_no_events();
    serverless.get_stats()
}

fn describe(stats: Stats, name: &str) {
    println!("describing {}", name);
    println!("{} successful invocations", stats.invocations);
    println!(
        "cold start rate = {}",
        (stats.cold_starts as f64) / (stats.invocations as f64)
    );
    println!(
        "wasted memory time = {}",
        *stats.wasted_resource_time.get("mem").unwrap()
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_azure_trace(Path::new(&args[1]), 100000);
    println!("trace processed successfully!");
    describe(test_policy(None, &trace), "No cold start policy");
    describe(
        test_policy(
            Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(20.0 * 60.0, None)))),
            &trace,
        ),
        "20-minute keepalive",
    );
    describe(
        test_policy(
            Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(0.0, Some(0.0))))),
            &trace,
        ),
        "No unloading",
    );
}
