/// Functions responsible for parsing Azure Functions 2019 trace and generating experiments using it.
/// Trace description: https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fs::read_dir;
use std::iter::repeat_with;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use csv::ReaderBuilder;
use indexmap::{IndexMap, IndexSet};
use rand::prelude::*;
use rand_pcg::Pcg64;
use rv::dist::{Empirical, Exponential, LogNormal};
use rv::traits::Rv;

use crate::trace::{ApplicationData, RequestData, Trace};

#[derive(Default, Clone, Copy)]
pub struct FunctionRecord {
    pub app_id: usize,
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

    fn function_iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.function_records.iter().map(|x| x.app_id))
    }

    fn is_ordered_by_time(&self) -> bool {
        true
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

/// Experiment generator will choose `count` random apps with popularity in range
/// [floor(`left` * n_apps), floor(`right` * n_apps)] (apps are sorted by popularity in decreasing order).
pub struct AppPreference {
    pub count: usize,
    pub left: f64,
    pub right: f64,
}

impl AppPreference {
    pub fn new(count: usize, left: f64, right: f64) -> Self {
        Self { count, left, right }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(0. ..1.).contains(&self.left) {
            return Err(format!("left position {} out of range [0, 1)", self.left));
        }
        if !(0. ..1.).contains(&self.right) {
            return Err(format!("right position {} out of range [0, 1)", self.right));
        }
        if self.left > self.right {
            return Err(format!(
                "left position {} greater than right position {}",
                self.left, self.right
            ));
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
pub enum DurationGenerator {
    /// Simple duration generator from quantiles.
    Piecewise,
    /// Generate duration from Lognormal(-0.38, 2.36).
    PrefittedLognormal,
    /// Generate duration from Lognormal distribution fitted to each function separately.
    Lognormal,
}

#[derive(PartialEq, Eq)]
pub enum StartGenerator {
    /// For each 1-minute bucket select each starting time uniformly at random within that bucket.
    BucketUniform,
    /// Fit Poisson process to buckets and generate the invocations from it.
    PoissonFit,
    /// Generate invocations from empirical distribution.
    EmpiricalFit,
}

pub struct Azure2019TraceConfig {
    /// Simulation time period in minutes (only integer numbers are supported).
    pub time_period: u64,
    /// This option allows skipping an integer number of minutes from the start of the trace.
    /// It may be useful if you want to split trace into small 1-hour experiments for example.
    pub time_skip: u64,
    /// This option controls the method used to generate execution durations.
    pub duration_generator: DurationGenerator,
    /// This option controls the method used to generate start times.
    pub start_generator: StartGenerator,
    /// This option controls which apps to use for trace generation.
    pub app_preferences: Vec<AppPreference>,
    /// This option sets concurrency level for all apps in the trace.
    pub concurrency_level: usize,
    /// This option sets the seed used to initialize random generator.
    pub random_seed: u64,
    /// This option sets name for the memory resource.
    pub memory_name: String,
    /// This option forces trace generator to use given amount of memory for all apps.
    pub force_fixed_memory: Option<u64>,
    /// Cold start latency, currently it's the same for all apps.
    pub cold_start_latency: f64,
    /// If `rps` is not None, trace generator attempts to scale trace to the given number of requests per second
    /// by either removing random requests or duplicating random requests (yes, that doesn't sound very solid).
    pub rps: Option<f64>,
}

impl Default for Azure2019TraceConfig {
    fn default() -> Self {
        Self {
            time_period: 60,
            time_skip: 0,
            duration_generator: DurationGenerator::Piecewise,
            start_generator: StartGenerator::BucketUniform,
            app_preferences: Default::default(),
            concurrency_level: 1,
            random_seed: 1,
            memory_name: "mem".to_string(),
            force_fixed_memory: None,
            cold_start_latency: 1.,
            rps: None,
        }
    }
}

/// This function parses Azure Functions 2019 trace and generates experiment.
pub fn process_azure_2019_trace(path: &Path, config: Azure2019TraceConfig) -> AzureTrace {
    for pref in config.app_preferences.iter() {
        if let Err(e) = pref.validate() {
            panic!("{}", e);
        }
    }
    let mut gen = Pcg64::seed_from_u64(config.random_seed);
    let mut parts = BTreeSet::<String>::new();
    let mut mem = HashMap::<String, PathBuf>::new();
    let mut inv = HashMap::<String, PathBuf>::new();
    let mut dur = HashMap::<String, PathBuf>::new();
    // parse trace directory
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
    let mut bad_parts = Vec::new();
    // here we filter trace because some days have incomplete information
    for part in parts.iter() {
        if !mem.contains_key(part) || !inv.contains_key(part) || !dur.contains_key(part) {
            bad_parts.push(part.clone());
        }
    }
    for part in bad_parts.iter() {
        parts.remove(part);
    }
    // now we need to find which trace parts cover the required time period
    let parts_needed = ((config.time_skip + config.time_period + 1439) / 1440) as usize;
    if parts.len() < parts_needed {
        panic!("Trace is too short for specified time range.");
    }
    let mut tail_part = ((config.time_period + config.time_skip) % 1440) as usize;
    if tail_part == 0 {
        tail_part = 1440;
    }
    let mut app_mem = HashMap::<String, usize>::new();
    // here we compute container memory for each app as the maximum historical memory usage
    for part in parts.iter().take(parts_needed) {
        let mut mem_file = ReaderBuilder::new()
            .from_path(mem.get(part).unwrap().as_path())
            .unwrap();
        for mem_rec in mem_file.records() {
            let record = mem_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            let val = usize::from_str(&record[record.len() - 1]).unwrap();
            let entry = app_mem.entry(id).or_default();
            *entry = usize::max(*entry, val);
        }
    }
    let mut app_popularity = HashMap::<String, u64>::new();
    // here we compute app popularity (by invocation count)
    for (part_id, part) in parts.iter().take(parts_needed).enumerate() {
        let begin = config.time_skip - config.time_skip.min((part_id as u64) * 1440);
        if begin >= 1440 {
            continue;
        }
        let bound = if part_id + 1 == parts_needed { tail_part } else { 1440 };
        let mut inv_file = ReaderBuilder::new()
            .from_path(inv.get(part).unwrap().as_path())
            .unwrap();
        for inv_rec in inv_file.records() {
            let record = inv_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            id.push('_');
            id.push_str(&record[2]);
            let app = app_id(&id);
            if !app_mem.contains_key(&app) {
                continue;
            }
            let mut cnt = 0;
            for i in (begin as usize)..bound {
                cnt += usize::from_str(&record[4 + i]).unwrap();
            }
            if cnt > 0 {
                *app_popularity.entry(app).or_default() += cnt as u64;
            }
        }
    }
    let mut app_pop_vec = Vec::<(String, u64)>::from_iter(app_popularity.iter().map(|x| (x.0.to_string(), *x.1)));
    app_pop_vec.sort_by_key(|x: &(String, u64)| -> (u64, String) { (x.1, x.0.clone()) });
    app_pop_vec.reverse();
    let all_apps = app_pop_vec.drain(..).map(|x| x.0).collect::<Vec<String>>();
    let mut apps = IndexSet::new();
    // now we have to filter the apps by app preferences
    for pref in config.app_preferences.iter() {
        let l = (pref.left * (all_apps.len() as f64)).floor() as usize;
        let r = (pref.right * (all_apps.len() as f64)).floor() as usize;
        if 1 + r - l < pref.count {
            panic!(
                "Not enough apps to satisfy preference ({}, {}, {})",
                pref.count, pref.left, pref.right
            );
        }
        apps.extend(all_apps[l..=r].choose_multiple(&mut gen, pref.count).cloned());
    }
    let mut fn_id = IndexMap::<String, usize>::new();
    let dur_percent = vec![0., 0.01, 0.25, 0.50, 0.75, 0.99, 1.];
    let mut invocations = Vec::<(usize, f64, f64)>::new();
    let mut poisson_scale_factor = 1.;
    // here we compute the scaling factor required for open-loop Poisson generation with RPS constraint
    if let Some(rps) = config.rps {
        if config.start_generator == StartGenerator::PoissonFit {
            let mut total = 0;
            for (part_id, part) in parts.iter().take(parts_needed).enumerate() {
                let bound = if part_id + 1 == parts_needed { tail_part } else { 1440 };
                let mut inv_file = ReaderBuilder::new()
                    .from_path(inv.get(part).unwrap().as_path())
                    .unwrap();
                for inv_rec in inv_file.records() {
                    let record = inv_rec.unwrap();
                    let mut id = record[0].to_string();
                    id.push('_');
                    id.push_str(&record[1]);
                    id.push('_');
                    id.push_str(&record[2]);
                    let app = app_id(&id);
                    if apps.contains(&app) {
                        for t in 0..bound {
                            let cnt = usize::from_str(&record[4 + t]).unwrap();
                            total += cnt;
                        }
                    }
                }
            }
            let need = (rps * (config.time_period as f64) * 60.).round();
            poisson_scale_factor = need / (total as f64);
        }
    }
    // here we generate invocations, their starting times and durations
    for (part_id, part) in parts.iter().take(parts_needed).enumerate() {
        let bound = if part_id + 1 == parts_needed { tail_part } else { 1440 };
        let mut inv_file = ReaderBuilder::new()
            .from_path(inv.get(part).unwrap().as_path())
            .unwrap();
        let mut dur_file = ReaderBuilder::new()
            .from_path(dur.get(part).unwrap().as_path())
            .unwrap();

        let mut inv_map = IndexMap::<String, (usize, usize)>::new();
        let mut poisson_data = IndexMap::<String, (usize, usize)>::new();
        for inv_rec in inv_file.records() {
            let record = inv_rec.unwrap();
            let mut id = record[0].to_string();
            id.push('_');
            id.push_str(&record[1]);
            id.push('_');
            id.push_str(&record[2]);
            let app = app_id(&id);
            if apps.contains(&app) {
                if config.start_generator == StartGenerator::BucketUniform {
                    let mut int_id = fn_id.len();
                    if let Some(idx) = fn_id.get(&id).copied() {
                        int_id = idx;
                    } else {
                        fn_id.insert(id.clone(), fn_id.len());
                    }
                    let begin = invocations.len();
                    let mut total = 0;
                    for t in 0..bound {
                        let cnt = usize::from_str(&record[4 + t]).unwrap();
                        total += cnt;
                        for _ in 0..cnt {
                            let second = gen.gen_range(0.0..1.0) * 60.0 + ((t * 60 + part_id * 1440 * 60) as f64);
                            invocations.push((int_id, second, 0.));
                        }
                    }
                    if total > 0 {
                        inv_map.insert(id.clone(), (begin, total));
                    }
                } else if config.start_generator == StartGenerator::EmpiricalFit {
                    let mut int_id = fn_id.len();
                    if let Some(idx) = fn_id.get(&id).copied() {
                        int_id = idx;
                    } else {
                        fn_id.insert(id.clone(), fn_id.len());
                    }
                    let begin = invocations.len();
                    let mut total = 0;
                    let mut samples = Vec::new();
                    for t in 0..bound {
                        let cnt = usize::from_str(&record[4 + t]).unwrap();
                        for _ in 0..cnt {
                            samples.push((t * 60) as f64);
                        }
                        total += cnt;
                    }
                    if total > 0 {
                        let dist = Empirical::new(samples);
                        for _ in 0..total {
                            let second =
                                dist.draw(&mut gen) + gen.gen_range(0.0..60.0) + ((part_id * 1440 * 60) as f64);
                            invocations.push((int_id, second, 0.));
                        }
                        inv_map.insert(id.clone(), (begin, total));
                    }
                } else {
                    let mut total = 0;
                    for t in 0..bound {
                        let cnt = usize::from_str(&record[4 + t]).unwrap();
                        total += cnt;
                    }
                    if total > 0 {
                        let entry = poisson_data.entry(id).or_default();
                        entry.0 += total;
                        entry.1 += bound;
                    }
                }
            }
        }
        if config.start_generator == StartGenerator::PoissonFit {
            let limit = (config.time_period as f64).min(((part_id + 1) as f64) * 1440.);
            for (id, (s, k)) in poisson_data.drain(..) {
                let lambda = (s as f64) / (k as f64) * poisson_scale_factor;
                let dist = Exponential::new(lambda).unwrap();
                let mut t = (part_id as f64) * 1440. + <Exponential as Rv<f64>>::draw(&dist, &mut gen);
                let begin = invocations.len();
                let mut int_id = fn_id.len();
                if let Some(idx) = fn_id.get(&id).copied() {
                    int_id = idx;
                } else {
                    fn_id.insert(id.clone(), fn_id.len());
                }
                while t < limit {
                    invocations.push((int_id, t * 60., 0.));
                    t += <Exponential as Rv<f64>>::draw(&dist, &mut gen);
                }
                if begin != invocations.len() {
                    inv_map.insert(id, (begin, invocations.len() - begin));
                }
            }
        }
        match config.duration_generator {
            DurationGenerator::Piecewise => {
                for dur_rec in dur_file.records() {
                    let record = dur_rec.unwrap();
                    let mut id = record[0].to_string();
                    id.push('_');
                    id.push_str(&record[1]);
                    id.push('_');
                    id.push_str(&record[2]);
                    if let Some((begin, len)) = inv_map.get(&id).copied() {
                        let mut perc = Vec::with_capacity(dur_percent.len());
                        for i in 7..record.len() {
                            let val = f64::from_str(&record[i]).unwrap();
                            perc.push(val);
                        }
                        for inv in &mut invocations[begin..begin + len] {
                            inv.2 = f64::max(0.001, gen_sample(&mut gen, &dur_percent, &perc) * 0.001);
                        }
                    }
                }
            }
            DurationGenerator::PrefittedLognormal => {
                let dist = LogNormal::new(-0.38, 2.36).unwrap();
                for inv in invocations.iter_mut() {
                    inv.2 = f64::max(0.001, dist.draw(&mut gen));
                }
            }
            DurationGenerator::Lognormal => {
                for dur_rec in dur_file.records() {
                    let record = dur_rec.unwrap();
                    let mut id = record[0].to_string();
                    id.push('_');
                    id.push_str(&record[1]);
                    id.push('_');
                    id.push_str(&record[2]);
                    if let Some((begin, len)) = inv_map.get(&id).copied() {
                        let mean = f64::from_str(&record[3]).unwrap() * 0.001;
                        let median = f64::from_str(&record[10]).unwrap() * 0.001;
                        let squared_dev = 2. * (mean / median).ln();
                        assert!(squared_dev >= 0.);
                        let dist = LogNormal::new(median.ln(), squared_dev.sqrt()).unwrap();
                        for inv in &mut invocations[begin..begin + len] {
                            inv.2 = f64::max(0.001, dist.draw(&mut gen));
                        }
                    }
                }
            }
        }
        // it's possible that some functions don't have a record in duration file
        // in this case we use Lognormal(-0.38, 2.36) distribution (log-normal MLE fit
        // from Serverless in the Wild paper)
        let dist = LogNormal::new(-0.38, 2.36).unwrap();
        for inv in invocations.iter_mut() {
            if inv.2 == 0. {
                inv.2 = f64::max(0.001, dist.draw(&mut gen));
            }
        }
    }
    let mut app_records: Vec<ApplicationRecord> = vec![Default::default(); apps.len()];
    let mut func_records: Vec<FunctionRecord> = vec![Default::default(); fn_id.len()];
    let mut app_indices = HashMap::<String, usize>::new();
    for (i, app) in apps.iter().enumerate() {
        app_records[i] = ApplicationRecord {
            mem: config
                .force_fixed_memory
                .unwrap_or_else(|| app_mem.get(app).copied().unwrap() as u64),
            cold_start: config.cold_start_latency,
        };
        app_indices.insert(app.clone(), i);
    }
    for (name, id) in fn_id.iter() {
        let app = app_id(name);
        func_records[*id] = FunctionRecord {
            app_id: *app_indices.get(&app).unwrap(),
        };
    }

    // if the trace has RPS constraint, we either remove random invocations
    // or copy random invocations to achieve given RPS rate
    if let Some(rps) = config.rps {
        let need = (rps * (config.time_period as f64) * 60.).round() as usize;
        match need.cmp(&invocations.len()) {
            Ordering::Less => {
                invocations.shuffle(&mut gen);
                invocations.truncate(need);
            }
            Ordering::Greater => {
                let mut tmp = repeat_with(|| invocations.choose(&mut gen).copied().unwrap())
                    .take(need - invocations.len())
                    .collect::<Vec<_>>();
                invocations.append(&mut tmp);
            }
            _ => {}
        }
    }

    invocations.sort_by(|x: &(usize, f64, f64), y: &(usize, f64, f64)| x.1.total_cmp(&y.1));
    let mut time_range = 0.0;
    for req in invocations.iter() {
        time_range = f64::max(time_range, req.1 + req.2);
    }
    let trace = invocations
        .drain(..)
        .map(|x| RequestData {
            id: x.0,
            duration: x.2,
            time: x.1,
        })
        .collect::<Vec<_>>();
    AzureTrace {
        concurrency_level: config.concurrency_level,
        memory_name: config.memory_name,
        sim_end: Some(time_range),
        trace_records: trace,
        function_records: func_records,
        app_records,
    }
}
