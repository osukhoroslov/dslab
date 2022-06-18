use std::fs::read_dir;
use std::path::Path;
use std::str::FromStr;

use csv::ReaderBuilder;

pub struct FunctionRow {
    pub time: u64,
    pub invocations: usize,
    pub exec: u32,
    pub cpu: usize,
    pub mem: usize,
    pub alloc_cpu: usize,
    pub alloc_mem: usize,
}

pub type FunctionTrace = Vec<FunctionRow>;
pub type OpenDCTrace = Vec<FunctionTrace>;

pub fn process_opendc_trace(path: &Path) -> OpenDCTrace {
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
            let row = FunctionRow {
                time: u64::from_str(&record[0]).unwrap(),
                invocations: usize::from_str(&record[1]).unwrap(),
                exec: u32::from_str(&record[2]).unwrap(),
                cpu: usize::from_str(&record[3]).unwrap(),
                mem: usize::from_str(&record[4]).unwrap(),
                alloc_cpu: usize::from_str(&record[5]).unwrap(),
                alloc_mem: usize::from_str(&record[6]).unwrap(),
            };
            fun_trace.push(row);
        }
        trace.push(fun_trace);
    }
    trace
}
