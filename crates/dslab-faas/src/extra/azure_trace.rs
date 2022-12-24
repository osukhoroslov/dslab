/// This file contains functions responsible for parsing Azure functions trace.
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use csv::ReaderBuilder;
use indexmap::IndexMap;
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::trace::{ApplicationData, RequestData, Trace};

#[derive(Default, Clone, Copy)]
pub struct FunctionRecord {
    pub app_id: u64,
}

#[derive(Default, Clone, Copy)]
pub struct ApplicationRecord {
    pub mem: u64,
    pub cold_start: f64,
}

#[derive(Default, Clone)]
pub struct AzureTrace {
    pub concurrency_level: usize,
    pub memory_name: String,
    pub sim_end: Option<f64>,
    pub trace_records: Vec<RequestData>,
    pub function_records: Vec<FunctionRecord>,
    pub app_records: Vec<ApplicationRecord>,
}

impl Trace for AzureTrace {
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_> {
        Box::new(self.app_records.iter().map(|x| {
            ApplicationData::new(
                self.concurrency_level,
                x.cold_start,
                1.0,
                vec![(self.memory_name.clone(), x.mem)],
            )
        }))
    }

    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_> {
        Box::new(self.trace_records.iter().cloned())
    }

    fn function_iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(self.function_records.iter().map(|x| x.app_id))
    }

    fn simulation_end(&self) -> Option<f64> {
        if self.sim_end.is_some() {
            self.sim_end
        } else {
            let mut time_range = 0.0;
            for req in self.trace_records.iter() {
                time_range = f64::max(time_range, req.time + req.duration);
            }
            Some(time_range)
        }
    }
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

pub struct AzureTraceConfig {
    pub invocations_limit: usize,
    pub concurrency_level: usize,
    pub memory_name: String,
}

impl Default for AzureTraceConfig {
    fn default() -> Self {
        Self {
            invocations_limit: 0,
            concurrency_level: 1,
            memory_name: "mem".to_string(),
        }
    }
}

/// This function parses Azure trace and generates experiment.
pub fn process_azure_trace(path: &Path, config: AzureTraceConfig) -> AzureTrace {
    let mut gen = Pcg64::seed_from_u64(1);
    let mut trace = Vec::new();
    let mut parts = BTreeSet::<String>::new();
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
    let mut fn_id = IndexMap::<String, usize>::new();
    let dur_percent = vec![0., 0.01, 0.25, 0.50, 0.75, 0.99, 1.];
    let mem_percent = vec![0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99, 1.];
    let limit = config.invocations_limit / parts.len();
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
        apps.sort_by_key(|x: &(String, u64)| -> (u64, String) { (x.1, x.0.clone()) });
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
            if !app_data.contains_key(&app) {
                app_data.insert(app.clone(), (app_data.len(), 0.1, mem as u64));
            }
            let mut funcs = Vec::new();
            for f in app_funcs.get_mut(&app).unwrap().drain() {
                funcs.push(f);
            }
            funcs.sort();
            for func in funcs.drain(..) {
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
                for (i, inv) in inv_vec.iter().copied().enumerate() {
                    for _ in 0..inv {
                        let second = gen.gen_range(0.0..1.0) * 60.0 + ((i * 60 + day * 1440 * 64) as f64);
                        let mut record = RequestData {
                            id: curr_id as u64,
                            duration: f64::max(0.001, gen_sample(&mut gen, &dur_percent, dur_vec) * 0.001),
                            time: second,
                        };
                        invocations_count += 1;
                        record.duration = (record.duration * 1000000.).round() / 1000000.;
                        record.time = (record.time * 1000000.).round() / 1000000.;
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
    trace.sort_by(|x: &RequestData, y: &RequestData| x.time.partial_cmp(&y.time).unwrap());
    let mut time_range = 0.0;
    for req in trace.iter() {
        time_range = f64::max(time_range, req.time + req.duration);
    }
    AzureTrace {
        concurrency_level: config.concurrency_level,
        memory_name: config.memory_name,
        sim_end: Some(time_range),
        trace_records: trace,
        function_records: funcs,
        app_records: apps,
    }
}
