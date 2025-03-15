//! DAG scheduling.

use std::cell::RefCell;
use std::rc::Rc;
use std::str::FromStr;

use indexmap::map::IndexMap;
use itertools::Itertools;

use simcore::component::Id;
use simcore::context::SimulationContext;

use crate::dag::DAG;
use crate::runner::Config;
use crate::system::System;
use crate::task::TaskState;

use crate::schedulers::dls::DlsScheduler;
use crate::schedulers::dynamic_list::DynamicListScheduler;
use crate::schedulers::heft::HeftScheduler;
use crate::schedulers::lookahead::LookaheadScheduler;
use crate::schedulers::peft::PeftScheduler;
use crate::schedulers::simple_scheduler::SimpleScheduler;

/// Represents an action ordered by the scheduler.
#[derive(Clone, Debug)]
pub enum Action {
    /// Execute the task on the resource using a given *number* of cores.
    ScheduleTask {
        task: usize,
        resource: usize,
        cores: u32,
        expected_span: Option<TimeSpan>,
    },
    /// Execute the task on the resource using a given *set* of cores.
    ScheduleTaskOnCores {
        task: usize,
        resource: usize,
        cores: Vec<u32>,
        expected_span: Option<TimeSpan>,
    },
    /// Transfer data item between the specified resources.
    /// Action will be queued if there is no such data item right now.
    TransferData { data_item: usize, from: Id, to: Id },
}

#[derive(Clone, Debug)]
pub struct TimeSpan {
    start: f64,
    finish: f64,
}

impl TimeSpan {
    pub fn new(start: f64, finish: f64) -> Self {
        Self { start, finish }
    }

    pub fn start(&self) -> f64 {
        self.start
    }
    pub fn finish(&self) -> f64 {
        self.finish
    }
    pub fn length(&self) -> f64 {
        self.finish - self.start
    }
}

/// Trait for implementing DAG scheduling algorithms.
///
/// Includes callback methods which can return one or multiple actions corresponding to decisions
/// made by the scheduler (assign task to resource, transfer data item between resources, etc).
pub trait Scheduler {
    /// Called once in the beginning of DAG execution.
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action>;
    /// Called on every task state change.
    ///
    /// Useful for implementing dynamic scheduling algorithms.
    /// For static algorithms just return `Vec::new()`.
    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        _dag: &DAG,
        _system: System,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        Vec::new()
    }

    /// Should be true iff on_task_state_chaged always returns empty vector.
    fn is_static(&self) -> bool;
}

pub type RcScheduler = Rc<RefCell<dyn Scheduler>>;

/// Contains parsed scheduler params.
#[derive(Debug, Clone)]
pub struct SchedulerParams {
    name: String,
    params: IndexMap<String, String>,
}

impl FromStr for SchedulerParams {
    type Err = String;

    /// Creates SchedulerParams from a string in the following format: `SchedulerName[param1=value1,param2=value2...]`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let open = s.find('[');
        if open.is_none() {
            return Ok(Self {
                name: s.to_string(),
                params: IndexMap::new(),
            });
        }

        let open = open.unwrap();
        if !s.ends_with(']') {
            return Err("Input string doesn't end with matching ]".to_string());
        }

        let mut params = IndexMap::new();
        for param in s[open + 1..s.len() - 1].split(',') {
            let pos = param.find('=').ok_or(format!("Can't find \"=\" in param {param}"))?;
            params.insert(param[..pos].to_string(), param[pos + 1..].to_string());
        }

        Ok(Self {
            name: s[..open].to_string(),
            params,
        })
    }
}

impl SchedulerParams {
    /// Returns scheduler name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns scheduler parameter by name.
    pub fn get<T: FromStr, K: AsRef<str>>(&self, name: K) -> Option<T> {
        self.params.get(name.as_ref()).and_then(|s| s.parse().ok())
    }
}

impl std::fmt::Display for SchedulerParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.params.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(
                f,
                "{}[{}]",
                self.name,
                self.params.iter().map(|(k, v)| format!("{k}={v}")).join(",")
            )
        }
    }
}

/// Resolves params into one of supported schedulers.
pub fn default_scheduler_resolver(params: &SchedulerParams) -> Option<RcScheduler> {
    match params.name.as_str() {
        "Simple" => Some(Rc::new(RefCell::new(SimpleScheduler::new()))),
        "DynamicList" => Some(Rc::new(RefCell::new(DynamicListScheduler::from_params(params)))),
        "HEFT" => Some(Rc::new(RefCell::new(HeftScheduler::from_params(params)))),
        "Lookahead" => Some(Rc::new(RefCell::new(LookaheadScheduler::from_params(params)))),
        "PEFT" => Some(Rc::new(RefCell::new(PeftScheduler::from_params(params)))),
        "DLS" => Some(Rc::new(RefCell::new(DlsScheduler::from_params(params)))),
        _ => None,
    }
}
