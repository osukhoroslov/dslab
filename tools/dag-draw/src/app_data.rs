use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use druid::{Data, Lens};
use serde::Deserialize;
use serde_json::Value;

use dslab_dag::trace_log::{Graph, TraceLog};

use crate::data::*;

#[derive(Clone, Copy, Data, PartialEq, Eq)]
pub enum NodeType {
    Task(usize),
    Input(usize),
    Output(usize),
}

#[derive(Clone, Data, Lens)]
pub struct AppData {
    pub slider: f64,
    pub total_time: f64,
    pub scheduler_files: Rc<RefCell<Vec<File>>>,
    pub compute: Rc<RefCell<Vec<Compute>>>,
    pub transfers: Rc<RefCell<Vec<Transfer>>>,
    pub task_info: Rc<RefCell<Vec<Option<TaskInfo>>>>,
    pub color_by_prefix: bool,
    pub files_limit_str: String,
    pub tasks_limit_str: String,
    pub timeline_downloading: bool,
    pub timeline_uploading: bool,
    pub timeline_cores: bool,
    pub timeline_memory: bool,
    pub timeline_merged_usages: bool,
    pub graph_levels_from_end: bool,
    pub graph_variable_edge_width: bool,
    pub graph_variable_node_size: bool,
    pub graph_show_task_names: bool,
    pub selected_node: Option<NodeType>,
    pub selected_node_info: String,
    pub graph: Rc<RefCell<Graph>>,
}

impl AppData {
    pub fn from_trace_log(trace_log: TraceLog) -> Self {
        let scheduler_files = Rc::new(RefCell::new(Vec::<File>::new()));
        let compute = Rc::new(RefCell::new(Vec::<Compute>::new()));
        let transfers = Rc::new(RefCell::new(Vec::<Transfer>::new()));
        let tasks_info = Rc::new(RefCell::new(vec![None; trace_log.graph.tasks.len()]));
        let mut total_time = 0.;

        let mut compute_index: HashMap<String, usize> = HashMap::new();

        // read compute actors
        for resource in trace_log.resources {
            let name = resource["name"].as_str().unwrap().to_string();
            compute_index.insert(name.clone(), compute.borrow().len());
            compute.borrow_mut().push(Compute {
                name,
                speed: resource["speed"].as_f64().unwrap(),
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
            match event["type"].as_str().unwrap() {
                "start_uploading" => {
                    uploads
                        .entry(event["data_id"].as_u64().unwrap())
                        .or_default()
                        .push(event.clone());
                    if event["from"].as_str().unwrap() == "runner" {
                        present_scheduler_files.insert(event["data_name"].as_str().unwrap().to_string());
                    }
                }
                "finish_uploading" => {
                    uploads
                        .entry(event["data_id"].as_u64().unwrap())
                        .or_default()
                        .push(event.clone());
                }
                "task_scheduled" => {
                    tasks
                        .entry(event["task_id"].as_u64().unwrap())
                        .or_default()
                        .push(event.clone());
                }
                "task_started" => {
                    tasks
                        .entry(event["task_id"].as_u64().unwrap())
                        .or_default()
                        .push(event.clone());
                }
                "task_completed" => {
                    tasks
                        .entry(event["task_id"].as_u64().unwrap())
                        .or_default()
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
            let name = events[0]["data_name"].as_str().unwrap().to_string();
            let start_time = events[0]["time"].as_f64().unwrap();
            let finish_time = events[1]["time"].as_f64().unwrap();
            let data_item_id = events[0]["data_item_id"].as_u64().unwrap() as usize;

            transfers.borrow_mut().push(Transfer {
                start: start_time,
                end: finish_time,
                from: source.clone(),
                to: destination.clone(),
                name: name.clone(),
                data_item_id,
            });

            if source == "runner" {
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
            let id = events[0]["task_id"].as_u64().unwrap() as usize;
            let actor = events[0]["location"].as_str().unwrap().to_string();

            tasks_info.borrow_mut()[id] = Some(TaskInfo {
                scheduled,
                started,
                completed,
                cores,
                id,
                name: trace_log.graph.tasks[id].name.clone(),
            });

            compute.borrow_mut()[*compute_index.get(&actor).unwrap()].tasks.push(id);
        }

        Self {
            slider: 0.0,
            total_time,
            scheduler_files,
            compute,
            transfers,
            task_info: tasks_info,
            color_by_prefix: false,
            files_limit_str: "10".to_string(),
            tasks_limit_str: "2".to_string(),
            timeline_downloading: true,
            timeline_uploading: true,
            timeline_cores: true,
            timeline_memory: true,
            timeline_merged_usages: false,
            graph_levels_from_end: false,
            graph_variable_edge_width: false,
            graph_variable_node_size: false,
            graph_show_task_names: false,
            selected_node: None,
            selected_node_info: "".to_string(),
            graph: Rc::new(RefCell::new(trace_log.graph)),
        }
    }
}

#[derive(Deserialize)]
pub struct AppDataSettings {
    color_by_prefix: Option<bool>,
    files_limit: Option<usize>,
    tasks_limit: Option<usize>,
    timeline_downloading: Option<bool>,
    timeline_uploading: Option<bool>,
    timeline_cores: Option<bool>,
    timeline_memory: Option<bool>,
    timeline_merged_usages: Option<bool>,
    graph_levels_from_end: Option<bool>,
    graph_variable_edge_width: Option<bool>,
    graph_variable_node_size: Option<bool>,
    graph_show_task_names: Option<bool>,
}

impl AppData {
    pub fn apply_settings(&mut self, settings: &AppDataSettings) {
        macro_rules! copy_setting {
            ($setting:ident) => {
                if let Some(x) = settings.$setting {
                    self.$setting = x;
                }
            };
        }

        copy_setting!(color_by_prefix);
        copy_setting!(timeline_downloading);
        copy_setting!(timeline_uploading);
        copy_setting!(timeline_cores);
        copy_setting!(timeline_memory);
        copy_setting!(timeline_merged_usages);
        copy_setting!(graph_levels_from_end);
        copy_setting!(graph_variable_edge_width);
        copy_setting!(graph_variable_node_size);
        copy_setting!(graph_show_task_names);

        if let Some(x) = settings.files_limit {
            self.files_limit_str = x.to_string();
        }
        if let Some(x) = settings.tasks_limit {
            self.tasks_limit_str = x.to_string();
        }
    }
}
