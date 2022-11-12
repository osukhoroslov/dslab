use std::fs::read_dir;
use std::path::Path;
use std::str::FromStr;

use csv::ReaderBuilder;

use crate::trace::{ApplicationData, RequestData, Trace};

#[derive(Default, Copy, Clone)]
pub struct FunctionSample {
    pub time: u64,
    pub invocations: usize,
    pub exec: u32,
    pub cpu_provisioned: usize,
    pub mem_provisioned: usize,
    pub cpu_used: usize,
    pub mem_used: usize,
}

pub type FunctionTrace = Vec<FunctionSample>;

pub struct OpenDCTrace {
    pub funcs: Vec<FunctionTrace>,
    pub concurrency_level: usize,
    pub cold_start: f64,
    pub memory_name: String,
    pub sim_end: Option<f64>,
}

impl Trace for OpenDCTrace {
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_> {
        Box::new(self.funcs.iter().map(|x| {
            let mut max_mem = 0;
            for sample in x.iter() {
                max_mem = usize::max(max_mem, sample.mem_provisioned);
            }
            ApplicationData::new(
                self.concurrency_level,
                self.cold_start,
                1.0,
                vec![(self.memory_name.clone(), max_mem as u64)],
            )
        }))
    }

    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_> {
        Box::new(OpenDCRequestIter::new(self.funcs.iter()))
    }

    fn function_iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(0..(self.funcs.len() as u64))
    }

    fn simulation_end(&self) -> Option<f64> {
        if self.sim_end.is_some() {
            self.sim_end
        } else {
            let mut end = 0.0;
            for fun in self.funcs.iter() {
                for sample in fun.iter() {
                    let t = ((sample.time + (sample.exec as u64)) as f64) / 1000.;
                    if sample.invocations > 0 && end < t {
                        end = t;
                    }
                }
            }
            Some(end)
        }
    }
}

pub struct OpenDCRequestIter<'a> {
    trace_iter: std::slice::Iter<'a, FunctionTrace>,
    trace: FunctionTrace,
    sample_id: usize,
    fn_id: usize,
    curr: FunctionSample,
    invocations: usize,
}

impl<'a> OpenDCRequestIter<'a> {
    pub fn new(trace_iter: std::slice::Iter<'a, FunctionTrace>) -> Self {
        Self {
            trace_iter,
            trace: Vec::new(),
            sample_id: 0,
            fn_id: 0,
            curr: Default::default(),
            invocations: 0,
        }
    }
}

impl<'a> Iterator for OpenDCRequestIter<'a> {
    type Item = RequestData;

    fn next(&mut self) -> Option<Self::Item> {
        while self.invocations == self.curr.invocations {
            while self.sample_id == self.trace.len() {
                if let Some(trace) = self.trace_iter.next() {
                    self.trace = trace.clone();
                    self.fn_id += 1;
                    self.sample_id = 0;
                } else {
                    return None;
                }
            }
            self.curr = self.trace[self.sample_id].clone();
            self.sample_id += 1;
            self.invocations = 0;
        }
        self.invocations += 1;
        Some(RequestData {
            id: (self.fn_id - 1) as u64,
            duration: (self.curr.exec as f64) / 1000.0,
            time: (self.curr.time as f64) / 1000.0,
        })
    }
}

pub struct OpenDCTraceConfig {
    pub concurrency_level: usize,
    pub cold_start: f64,
    pub memory_name: String,
}

impl Default for OpenDCTraceConfig {
    fn default() -> Self {
        Self {
            concurrency_level: 1,
            cold_start: 0.0,
            memory_name: "mem".to_string(),
        }
    }
}

pub fn process_opendc_trace(path: &Path, config: OpenDCTraceConfig) -> OpenDCTrace {
    let mut files = Vec::new();
    match read_dir(path) {
        Ok(paths) => {
            for entry_ in paths {
                let entry = entry_.unwrap();
                let path = entry.path();
                if !path.as_path().is_file() {
                    continue;
                }
                files.push(path);
            }
        }
        Err(e) => {
            panic!("error while reading trace dir: {}", e);
        }
    }
    let mut trace = Vec::new();
    for f in files {
        let mut fun_trace = Vec::new();
        let mut file = ReaderBuilder::new().from_path(f.as_path()).unwrap();
        for rec in file.records() {
            let record = rec.unwrap();
            let row = FunctionSample {
                time: u64::from_str(&record[0]).unwrap(),
                invocations: usize::from_str(&record[1]).unwrap(),
                exec: u32::from_str(&record[2]).unwrap(),
                cpu_provisioned: usize::from_str(&record[3]).unwrap(),
                mem_provisioned: usize::from_str(&record[4]).unwrap(),
                cpu_used: usize::from_str(&record[5]).unwrap(),
                mem_used: usize::from_str(&record[6]).unwrap(),
            };
            fun_trace.push(row);
        }
        trace.push(fun_trace);
    }
    let mut end = 0.0;
    for fun in trace.iter() {
        for sample in fun.iter() {
            let t = ((sample.time + (sample.exec as u64)) as f64) / 1000.;
            if sample.invocations > 0 && end < t {
                end = t;
            }
        }
    }
    OpenDCTrace {
        funcs: trace,
        concurrency_level: config.concurrency_level,
        cold_start: config.cold_start,
        memory_name: config.memory_name,
        sim_end: Some(end),
    }
}
