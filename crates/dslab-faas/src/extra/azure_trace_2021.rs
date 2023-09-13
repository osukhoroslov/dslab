//! Functions for parsing Azure Functions 2021 trace and generating experiments using it.
//!
//! Trace description: <https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsInvocationTrace2021.md>
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use csv::ReaderBuilder;
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::extra::azure_trace_2019::{ApplicationRecord, AzureTrace, FunctionRecord};
use crate::trace::RequestData;

/// Samples one value from Burr(c, k, lambda) distribution.
pub fn burr_sample<R: Rng>(c: f64, k: f64, lambda: f64, rng: &mut R) -> f64 {
    let u: f64 = rng.gen();
    lambda * ((1. - u).powf(-1. / k) - 1.).powf(1. / c)
}

/// Generator of application memory requirements.
pub enum MemoryGenerator {
    /// All apps use fixed amount of memory.
    Fixed(u64),
    /// Burr fit from Serverless in the Wild paper.
    /// c = 11.652, k = 0.221, lambda = 107.083
    PrefittedBurr,
}

/// Struct with Azure 2021 trace settings.
pub struct Azure2021TraceConfig {
    /// Simulation time period in seconds.
    pub time_period: f64,
    /// This option allows skipping a number of seconds from the start of the trace.
    pub time_skip: f64,
    /// This option controls the method used to generate app memory usage.
    /// Note that the trace has no information about memory.
    pub memory_generator: MemoryGenerator,
    /// This option sets concurrency level for all apps in the trace.
    pub concurrency_level: usize,
    /// This option sets the seed used to initialize random generator.
    pub random_seed: u64,
    /// This option sets name for the memory resource.
    pub memory_name: String,
    /// Cold start latency, currently it's the same for all apps.
    pub cold_start_latency: f64,
}

impl Default for Azure2021TraceConfig {
    fn default() -> Self {
        Self {
            time_period: 3600.,
            time_skip: 0.,
            memory_generator: MemoryGenerator::PrefittedBurr,
            concurrency_level: 1,
            random_seed: 1,
            memory_name: "mem".to_string(),
            cold_start_latency: 1.,
        }
    }
}

/// This function parses Azure Function 2021 trace and generates experiment.
pub fn process_azure_2021_trace(path: &Path, config: Azure2021TraceConfig) -> AzureTrace {
    let mut gen = Pcg64::seed_from_u64(config.random_seed);
    let mut file = ReaderBuilder::new().from_path(path).unwrap();
    // data: (id, app id)
    let mut func_data = HashMap::<String, (usize, usize)>::new();
    // data: (id, memory)
    let mut app_data = HashMap::<String, (usize, u64)>::new();
    let mut invocations = Vec::<RequestData>::new();
    let mut func_records = Vec::<FunctionRecord>::new();
    let mut app_records = Vec::<ApplicationRecord>::new();
    for rec in file.records() {
        let record = rec.unwrap();
        let end = f64::from_str(&record[2]).unwrap();
        let mut duration = f64::from_str(&record[3]).unwrap();
        let start = end - duration;
        duration = f64::max(0.001, duration);
        if start < config.time_skip || start > config.time_skip + config.time_period {
            continue;
        }
        let app = record[0].to_string();
        let func = record[1].to_string();
        if !app_data.contains_key(&app) {
            let mem = match config.memory_generator {
                MemoryGenerator::Fixed(x) => x,
                MemoryGenerator::PrefittedBurr => burr_sample(11.652, 0.221, 107.083, &mut gen).ceil() as u64,
            };
            app_records.push(ApplicationRecord {
                mem,
                cold_start: config.cold_start_latency,
            });
            app_data.insert(app.clone(), (app_data.len(), mem));
        }
        let app_id = app_data.get(&app).unwrap().0;
        if !func_data.contains_key(&func) {
            func_records.push(FunctionRecord { app_id });
            func_data.insert(func.clone(), (func_data.len(), app_id));
        }
        let func_id = func_data.get(&func).unwrap().0;
        invocations.push(RequestData {
            id: func_id,
            duration,
            time: start,
        });
    }
    invocations.sort_by(|a, b| a.time.total_cmp(&b.time));

    let mut time_range = 0.0;
    for req in invocations.iter() {
        time_range = f64::max(time_range, req.time + req.duration);
    }
    AzureTrace {
        concurrency_level: config.concurrency_level,
        memory_name: config.memory_name,
        sim_end: Some(time_range),
        trace_records: invocations,
        function_records: func_records,
        app_records,
    }
}
