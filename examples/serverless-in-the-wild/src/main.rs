use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;

use csv::ReaderBuilder;
use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_core::simulation::Simulation;
use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::function::{Application, Function};
use dslab_faas::resource::{ResourceConsumer, ResourceProvider};
use dslab_faas::simulation::ServerlessSimulation;
use dslab_faas::stats::Stats;
use dslab_faas_extra::hybrid_histogram::HybridHistogramPolicy;

#[derive(Default, Clone, Copy)]
struct TraceRecord {
    pub id: usize,
    pub time: f64,
    pub dur: f64,
}

#[derive(Default, Clone, Copy)]
struct FunctionRecord {
    pub app_id: u64,
}

#[derive(Default, Clone, Copy)]
struct ApplicationRecord {
    pub mem: u64,
    pub cold_start: f64,
}

#[derive(Default, Clone)]
struct Trace {
    pub trace_records: Vec<TraceRecord>,
    pub function_records: Vec<FunctionRecord>,
    pub app_records: Vec<ApplicationRecord>,
}

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

fn app_id(id: &str) -> String {
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
    id0
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
    // values: (id, coldstart_latency, memory)
    let mut app_data = HashMap::<String, (usize, f64, u64)>::new();
    let mut fn_id = HashMap::<String, usize>::new();
    let dur_percent = vec![0., 0.01, 0.25, 0.50, 0.75, 0.99, 1.];
    let mem_percent = vec![0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1.];
    let limit = invocations_limit / parts.len();
    for part in parts.iter() {
        if !mem.contains_key(part) {
            continue;
        }
        let mut invocations_count = 0;
        let mut mem_file = ReaderBuilder::new()
            .from_path(mem.get(part).unwrap().as_path())
            .unwrap();
        let mut inv_file = ReaderBuilder::new()
            .from_path(inv.get(part).unwrap().as_path())
            .unwrap();
        let mut dur_file = ReaderBuilder::new()
            .from_path(dur.get(part).unwrap().as_path())
            .unwrap();
        let mut app_funcs = HashMap::<String, HashSet<String>>::new();
        let mut app_popularity = HashMap::<String, u64>::new();
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
            let app = app_id(&id);
            if !app_funcs.contains_key(&app) {
                app_funcs.insert(app.clone(), HashSet::new());
                app_popularity.insert(app.clone(), 0);
            }
            app_funcs.get_mut(&app).unwrap().insert(id.clone());
            let mut cnt = Vec::with_capacity(1440);
            for i in 0..1440 {
                cnt.push(usize::from_str(&record[4 + i]).unwrap());
            }
            *app_popularity.get_mut(&app).unwrap() += cnt.iter().sum::<usize>() as u64;
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
        let mut apps = Vec::<(String, u64)>::from_iter(app_popularity.iter().map(|x| (x.0.to_string(), *x.1)));
        apps.sort_by_key(|x: &(String, u64)| -> u64 { x.1 });
        apps.reverse();
        //median popularity apps
        let mid = apps.len() / 2 - 40;
        let day = usize::from_str(&part[1..]).unwrap() - 1;
        for (app, _) in apps.drain(mid..) {
            if invocations_count == limit {
                break;
            }
            if !mem_dist.contains_key(&app) {
                continue;
            }
            let mem_vec = mem_dist.get(&app).unwrap();
            let mem = gen_sample(&mut gen, &mem_percent, mem_vec);
            app_data.insert(app.clone(), (app_data.len(), 0.1, mem as u64));
            for func in app_funcs.get_mut(&app).unwrap().drain() {
                if invocations_count == limit {
                    break;
                }
                let curr_id = fn_id.len();
                fn_id.insert(func.clone(), curr_id);
                if !dur_dist.contains_key(&func) {
                    continue;
                }
                let dur_vec = dur_dist.get(&func).unwrap();
                let inv_vec = inv_cnt.get(&func).unwrap();
                for i in 0..1440 {
                    for _ in 0..inv_vec[i] {
                        let second = gen.gen_range(0.0..1.0) * 60.0 + ((i * 60 + day * 1440 * 64) as f64);
                        let record = TraceRecord {
                            id: curr_id,
                            time: second,
                            dur: gen_sample(&mut gen, &dur_percent, dur_vec) * 0.001,
                        };
                        invocations_count += 1;
                        trace.push(record);
                        if invocations_count == limit {
                            break;
                        }
                    }
                    if invocations_count == limit {
                        break;
                    }
                }
            }
        }
    }
    let mut apps: Vec<ApplicationRecord> = vec![Default::default(); app_data.len()];
    let mut funcs: Vec<FunctionRecord> = vec![Default::default(); fn_id.len()];
    for (_, data) in app_data.iter() {
        apps[data.0] = ApplicationRecord {
            mem: data.2,
            cold_start: data.1,
        };
    }
    for (name, id) in fn_id.iter() {
        let app = app_id(name);
        funcs[*id] = FunctionRecord {
            app_id: app_data.get(&app).unwrap().0 as u64,
        };
    }
    trace.sort_by(|x: &TraceRecord, y: &TraceRecord| x.time.total_cmp(&y.time));
    Trace {
        trace_records: trace,
        function_records: funcs,
        app_records: apps,
    }
}

fn test_policy(policy: Option<Rc<RefCell<dyn ColdStartPolicy>>>, trace: &Trace) -> Stats {
    let mut time_range = 0.0;
    for req in trace.trace_records.iter() {
        time_range = f64::max(time_range, req.time + req.dur);
    }
    let sim = Simulation::new(1);
    let mut serverless = ServerlessSimulation::new(sim, None, policy, None);
    for _ in 0..1000 {
        let mem = serverless.create_resource("mem", 4096 * 4);
        serverless.add_host(None, ResourceProvider::new(vec![mem]));
    }
    for app in trace.app_records.iter() {
        let mem = serverless.create_resource_requirement("mem", app.mem);
        serverless.add_app(Application::new(16, app.cold_start, ResourceConsumer::new(vec![mem])));
    }
    for func in trace.function_records.iter() {
        serverless.add_function(Function::new(func.app_id));
    }
    for req in trace.trace_records.iter() {
        serverless.send_invocation_request(req.id as u64, req.dur, req.time);
    }
    serverless.set_simulation_end(time_range);
    serverless.step_until_no_events();
    serverless.get_stats()
}

fn print_results(stats: Stats, name: &str) {
    println!("describing {}", name);
    println!("{} successful invocations", stats.invocations);
    println!(
        "cold start rate = {}",
        (stats.cold_starts as f64) / (stats.invocations as f64)
    );
    println!("wasted memory time = {}", *stats.wasted_resource_time.get(&0).unwrap());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace = process_azure_trace(Path::new(&args[1]), 200000);
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    print_results(
        test_policy(
            Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(
                f64::MAX / 10.0,
                0.0,
            )))),
            &trace,
        ),
        "No unloading",
    );
    for len in vec![20.0, 45.0, 60.0, 90.0, 120.0] {
        print_results(
            test_policy(
                Some(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0)))),
                &trace,
            ),
            &format!("{}-minute keepalive", len),
        );
    }
    for len in vec![2.0, 3.0, 4.0] {
        print_results(
            test_policy(
                Some(Rc::new(RefCell::new(HybridHistogramPolicy::new(
                    3600.0 * len,
                    60.0,
                    2.0,
                    0.5,
                    0.15,
                    0.1,
                )))),
                &trace,
            ),
            &format!("Hybrid Histogram policy, {} hours bound", len),
        );
    }
}
