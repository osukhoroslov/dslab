use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use druid::{Color, Data, Lens};

use serde_json::Value;

use crate::data::*;

#[derive(Clone, Data, Lens)]
pub struct AppData {
    pub slider: f64,
    pub total_time: f64,
    pub scheduler_files: Rc<RefCell<Vec<File>>>,
    pub compute: Rc<RefCell<Vec<Compute>>>,
    pub transfers: Rc<RefCell<Vec<Transfer>>>,
    pub files_limit_str: String,
    pub tasks_limit_str: String,
}

impl AppData {
    pub fn from_trace_log(trace_log: TraceLog) -> Self {
        // some random colors
        let colors = vec![
            Color::from_hex_str("3FA7D6").unwrap(),
            Color::from_hex_str("FAC05E").unwrap(),
            Color::from_hex_str("59CD90").unwrap(),
            Color::from_hex_str("F79D84").unwrap(),
            Color::from_hex_str("619B8A").unwrap(),
            Color::from_hex_str("FDF5BF").unwrap(),
            Color::from_hex_str("8BB8A8").unwrap(),
            Color::from_hex_str("A11692").unwrap(),
            Color::from_hex_str("4A5899").unwrap(),
            Color::from_hex_str("DDBEA8").unwrap(),
        ];

        let mut icolor: usize = 0;

        let mut get_next_color = || -> Color {
            let color = colors[icolor].clone();
            icolor = (icolor + 1) % colors.len();
            color
        };

        let scheduler_files = Rc::new(RefCell::new(Vec::<File>::new()));
        let compute = Rc::new(RefCell::new(Vec::<Compute>::new()));
        let transfers = Rc::new(RefCell::new(Vec::<Transfer>::new()));
        let mut total_time = 0.;

        let mut compute_index: HashMap<String, usize> = HashMap::new();

        // read compute actors
        for resource in trace_log.resources {
            let name = resource["id"].as_str().unwrap().to_string();
            compute_index.insert(name.clone(), compute.borrow().len());
            compute.borrow_mut().push(Compute {
                name,
                speed: resource["speed"].as_u64().unwrap(),
                cores: resource["cores"].as_u64().unwrap() as u32,
                memory: resource["memory"].as_u64().unwrap(),
                files: Vec::new(),
                tasks: Vec::new(),
            });
        }

        let mut uploads: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        let mut tasks: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
        let mut present_scheduler_files: BTreeSet<String> = BTreeSet::new();

        // split events into groups by id and general type (task/file)
        for event in trace_log.events.iter() {
            let time = event["time"].as_f64().unwrap();
            total_time = time;
            match event["type"].as_str().unwrap().as_ref() {
                "start_uploading" => {
                    uploads
                        .entry(event["id"].as_u64().unwrap())
                        .or_insert(Vec::new())
                        .push(event.clone());
                    if event["from"].as_str().unwrap() == "scheduler" {
                        present_scheduler_files.insert(event["name"].as_str().unwrap().to_string());
                    }
                }
                "finish_uploading" => {
                    uploads
                        .entry(event["id"].as_u64().unwrap())
                        .or_insert(Vec::new())
                        .push(event.clone());
                }
                "task_scheduled" => {
                    tasks
                        .entry(event["id"].as_u64().unwrap())
                        .or_insert(Vec::new())
                        .push(event.clone());
                }
                "task_started" => {
                    tasks
                        .entry(event["id"].as_u64().unwrap())
                        .or_insert(Vec::new())
                        .push(event.clone());
                }
                "task_completed" => {
                    tasks
                        .entry(event["id"].as_u64().unwrap())
                        .or_insert(Vec::new())
                        .push(event.clone());
                }
                _ => {}
            }
        }

        for (_id, events) in uploads.iter() {
            if events.len() != 2
                || events[0]["type"].as_str().unwrap() != "start_uploading"
                || events[1]["type"].as_str().unwrap() != "finish_uploading"
            {
                eprintln!(
                    "must be exactly 2 events for uploading: start_uploading and finish_uploading, found {:?}",
                    events
                );
            }

            let source = events[0]["from"].as_str().unwrap().to_string();
            let destination = events[0]["to"].as_str().unwrap().to_string();
            let name = events[0]["name"].as_str().unwrap().to_string();
            let start_time = events[0]["time"].as_f64().unwrap();
            let finish_time = events[1]["time"].as_f64().unwrap();

            transfers.borrow_mut().push(Transfer {
                start: start_time,
                end: finish_time,
                from: source.clone(),
                to: destination.clone(),
                name: name.clone(),
            });

            if source == "scheduler" {
                compute.borrow_mut()[*compute_index.get(&destination).unwrap()]
                    .files
                    .push(File {
                        start: start_time,
                        uploaded: finish_time,
                        end: total_time,
                        name,
                    });
            } else {
                compute.borrow_mut()[*compute_index.get(&source).unwrap()]
                    .files
                    .push(File {
                        start: start_time,
                        uploaded: start_time,
                        end: total_time,
                        name: name.clone(),
                    });
                present_scheduler_files.remove(&name);
                scheduler_files.borrow_mut().push(File {
                    start: start_time,
                    uploaded: finish_time,
                    end: total_time,
                    name,
                });
            }
        }

        let mut extra_scheduler_files: Vec<File> = Vec::new();
        for name in present_scheduler_files {
            extra_scheduler_files.push(File {
                start: 0.,
                uploaded: -1.,
                end: total_time,
                name: name.clone(),
            });
        }
        extra_scheduler_files.append(&mut scheduler_files.borrow_mut());
        std::mem::swap(&mut extra_scheduler_files, &mut scheduler_files.borrow_mut());

        for (_id, events) in tasks.iter() {
            if events.len() != 3
                || events[0]["type"].as_str().unwrap() != "task_scheduled"
                || events[1]["type"].as_str().unwrap() != "task_started"
                || events[2]["type"].as_str().unwrap() != "task_completed"
            {
                eprintln!(
                    "must be exactly 3 events for task: task_scheduled, task_started and task_completed, found {:?}",
                    events
                );
            }

            let scheduled = events[0]["time"].as_f64().unwrap();
            let started = events[1]["time"].as_f64().unwrap();
            let completed = events[2]["time"].as_f64().unwrap();
            let cores = events[0]["cores"].as_u64().unwrap() as u32;
            let memory = events[0]["memory"].as_u64().unwrap();
            let name = events[0]["name"].as_str().unwrap().to_string();
            let actor = events[0]["location"].as_str().unwrap().to_string();
            compute.borrow_mut()[*compute_index.get(&actor).unwrap()]
                .tasks
                .push(Task {
                    scheduled,
                    started,
                    completed,
                    cores,
                    memory,
                    name,
                    color: get_next_color(),
                });
        }

        Self {
            slider: 0.0,
            total_time,
            scheduler_files,
            compute,
            transfers,
            files_limit_str: "10".to_string(),
            tasks_limit_str: "2".to_string(),
        }
    }
}
