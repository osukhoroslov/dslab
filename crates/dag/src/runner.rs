use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde::Serialize;
use serde_json::json;

use enum_iterator::IntoEnumIterator;

use compute::multicore::*;
use network::model::DataTransferCompleted;
use network::network::Network;
use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::{log_error, log_info};

use crate::dag::DAG;
use crate::data_item::DataItemState;
use crate::resource::Resource;
use crate::scheduler::{Action, Scheduler};
use crate::task::TaskState;
use crate::trace_log::TraceLog;

#[derive(Clone, PartialEq)]
pub enum DataTransferMode {
    ViaMasterNode,
    Direct,
}

impl DataTransferMode {
    pub fn net_time(&self, network: &Network, src: Id, dst: Id, runner: Id) -> f64 {
        match self {
            DataTransferMode::ViaMasterNode => {
                1. / network.bandwidth(src, runner) + 1. / network.bandwidth(runner, dst)
            }
            DataTransferMode::Direct => 1. / network.bandwidth(src, dst),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub data_transfer_mode: DataTransferMode,
}

struct DataTransfer {
    data_id: usize,
    task_id: usize,
    from: Id,
    #[allow(dead_code)]
    to: Id,
}

#[derive(Clone, Debug)]
struct QueuedTask {
    task_id: usize,
    cores: u32,
    action_id: usize,
}

pub struct DAGRunner {
    id: Id,
    dag: DAG,
    network: Rc<RefCell<Network>>,
    resources: Vec<Resource>,
    computations: HashMap<u64, usize>,
    task_location: HashMap<usize, usize>,
    data_transfers: HashMap<usize, DataTransfer>,
    data_location: HashMap<usize, Id>,
    outputs: HashSet<usize>,
    task_cores: HashMap<usize, Vec<u32>>,
    task_inputs: HashMap<usize, HashSet<usize>>,
    trace_log: TraceLog,
    scheduler: Rc<RefCell<dyn Scheduler>>,
    actions: VecDeque<Action>,
    action_id: usize,
    scheduled_actions: HashSet<usize>,
    resource_queue: Vec<Vec<VecDeque<QueuedTask>>>,
    available_cores: Vec<BTreeSet<u32>>,
    trace_log_enabled: bool,
    config: Config,
    ctx: SimulationContext,
}

impl DAGRunner {
    pub fn new(
        dag: DAG,
        network: Rc<RefCell<Network>>,
        resources: Vec<Resource>,
        scheduler: Rc<RefCell<dyn Scheduler>>,
        config: Config,
        ctx: SimulationContext,
    ) -> Self {
        let resource_queue = resources
            .iter()
            .map(|resource| {
                (0..resource.compute.borrow().cores_total())
                    .map(|_| VecDeque::new())
                    .collect()
            })
            .collect();
        let available_cores = resources
            .iter()
            .map(|resource| (0..resource.compute.borrow().cores_total()).collect())
            .collect();
        Self {
            id: ctx.id(),
            dag,
            network,
            resources,
            computations: HashMap::new(),
            task_location: HashMap::new(),
            data_transfers: HashMap::new(),
            data_location: HashMap::new(),
            outputs: HashSet::new(),
            task_cores: HashMap::new(),
            task_inputs: HashMap::new(),
            trace_log: TraceLog::new(),
            scheduler,
            actions: VecDeque::new(),
            action_id: 0 as usize,
            scheduled_actions: HashSet::new(),
            resource_queue,
            available_cores,
            trace_log_enabled: true,
            config,
            ctx,
        }
    }

    pub fn enable_trace_log(&mut self, flag: bool) {
        self.trace_log_enabled = flag;
    }

    pub fn start(&mut self) {
        for (id, data_item) in self.dag.get_data_items().iter().enumerate() {
            if data_item.state == DataItemState::Ready {
                assert!(data_item.is_input, "Non-input data item has Ready state");
                self.data_location.insert(id, self.id);
            } else if data_item.consumers.len() == 0 {
                self.outputs.insert(id);
            }
        }

        log_info!(
            self.ctx,
            "started DAG execution: total {} resources, {} tasks, {} data items",
            self.resources.len(),
            self.dag.get_tasks().len(),
            self.dag.get_data_items().len()
        );
        self.trace_config();
        self.actions.extend(self.scheduler.borrow_mut().start(
            &self.dag,
            &self.resources,
            &self.network.borrow(),
            self.config.clone(),
            &self.ctx,
        ));
        self.process_actions();
    }

    fn trace_config(&mut self) {
        if !self.trace_log_enabled {
            return;
        }
        for resource in self.resources.iter() {
            self.trace_log.resources.push(json!({
                "name": resource.name.clone(),
                "speed": resource.compute.borrow().speed(),
                "cores": resource.cores_available,
                "memory": resource.memory_available,
            }));
        }
        self.trace_log.log_dag(&self.dag);
    }

    pub fn trace_log(&self) -> &TraceLog {
        &self.trace_log
    }

    fn process_schedule_action(&mut self, task: usize, resource: usize, need_cores: u32, allowed_cores: Vec<u32>) {
        if need_cores > self.resources[resource].compute.borrow().cores_total() {
            log_error!(
                self.ctx,
                "Wrong action, resource {} doesn't have enough cores",
                resource
            );
            return;
        }
        let task_id = task;
        let task = self.dag.get_task(task_id);
        if task.memory > self.resources[resource].compute.borrow().memory_total() {
            log_error!(
                self.ctx,
                "Wrong action, resource {} doesn't have enough memory",
                resource
            );
            return;
        }
        if need_cores < task.min_cores || task.max_cores < need_cores {
            log_error!(
                self.ctx,
                "Wrong action, task {} doesn't support {} cores",
                task_id,
                need_cores
            );
            return;
        }
        if task.state == TaskState::Ready {
            self.dag.update_task_state(task_id, TaskState::Runnable);
        } else if task.state == TaskState::Pending {
            self.dag.update_task_state(task_id, TaskState::Scheduled);
        } else {
            log_error!(self.ctx, "Can't schedule task with state {:?}", task.state);
            return;
        }
        for core in allowed_cores.into_iter() {
            self.resource_queue[resource][core as usize].push_back(QueuedTask {
                task_id,
                cores: need_cores,
                action_id: self.action_id,
            });
        }
        self.process_resource_queue(resource);
    }

    fn process_actions(&mut self) {
        for i in 0..self.resources.len() {
            self.process_resource_queue(i);
        }
        while !self.actions.is_empty() {
            match self.actions.pop_front().unwrap() {
                Action::Schedule { task, resource, cores } => {
                    let allowed_cores =
                        (0..self.resources[resource].compute.borrow().cores_total()).collect::<Vec<_>>();
                    self.process_schedule_action(task, resource, cores, allowed_cores);
                }
                Action::ScheduleOnCores {
                    task,
                    resource,
                    mut cores,
                } => {
                    cores.sort();
                    if cores.windows(2).any(|window| window[0] == window[1]) {
                        log_error!(self.ctx, "Wrong action, cores list {:?} contains same cores", cores);
                        return;
                    }
                    self.process_schedule_action(task, resource, cores.len() as u32, cores);
                }
            };
            self.action_id += 1;
        }
    }

    fn process_resource_queue(&mut self, resource_idx: usize) {
        while !self.resource_queue[resource_idx].is_empty() {
            let mut something_scheduled = false;

            let mut needed_cores: BTreeMap<usize, u32> = BTreeMap::new();
            let mut task_ids: BTreeMap<usize, usize> = BTreeMap::new();
            let mut ready_cores: BTreeMap<usize, Vec<u32>> = BTreeMap::new();
            for &core in self.available_cores[resource_idx].iter() {
                while !self.resource_queue[resource_idx][core as usize].is_empty()
                    && self
                        .scheduled_actions
                        .contains(&self.resource_queue[resource_idx][core as usize][0].action_id)
                {
                    self.resource_queue[resource_idx][core as usize].pop_front();
                }
                if self.resource_queue[resource_idx][core as usize].is_empty() {
                    continue;
                }

                let queued_task = &self.resource_queue[resource_idx][core as usize][0];
                let task = self.dag.get_task(queued_task.task_id);
                if task.memory > self.resources[resource_idx].memory_available {
                    continue;
                }
                if task.state != TaskState::Runnable {
                    continue;
                }

                needed_cores.insert(queued_task.action_id, queued_task.cores);
                task_ids.insert(queued_task.action_id, queued_task.task_id);
                ready_cores.entry(queued_task.action_id).or_default().push(core);
            }

            for (action_id, need_cores) in needed_cores.into_iter() {
                let mut ready_cores = ready_cores.remove(&action_id).unwrap();
                ready_cores.truncate(need_cores as usize);
                if ready_cores.len() < need_cores as usize {
                    continue;
                }

                for &core in ready_cores.iter() {
                    self.resource_queue[resource_idx][core as usize].pop_front();
                    self.available_cores[resource_idx].remove(&core);
                }

                let task_id = task_ids.remove(&action_id).unwrap();
                let task = self.dag.get_task(task_id);
                let mut resource = &mut self.resources[resource_idx];
                resource.cores_available -= need_cores;
                resource.memory_available -= task.memory;
                let resource = &self.resources[resource_idx];
                self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
                self.task_cores.insert(task_id, ready_cores);
                for &data_id in task.inputs.iter() {
                    let data_item = self.dag.get_data_item(data_id);
                    let data_location = *self.data_location.get(&data_id).unwrap();
                    let data_event_id = self.network.borrow_mut().transfer_data(
                        data_location,
                        resource.id,
                        data_item.size as f64,
                        self.id,
                    );
                    self.data_transfers.insert(
                        data_event_id,
                        DataTransfer {
                            data_id,
                            task_id,
                            from: self.id,
                            to: resource.id,
                        },
                    );
                    if self.trace_log_enabled {
                        self.trace_log.log_event(
                            &self.ctx,
                            json!({
                                "time": self.ctx.time(),
                                "type": "start_uploading",
                                "from": self.ctx.lookup_name(data_location),
                                "to": resource.name.clone(),
                                "data_id": data_event_id,
                                "data_name": data_item.name.clone(),
                                "task_id": task_id,
                            }),
                        );
                    }
                }
                self.task_location.insert(task_id, resource_idx);
                if self.trace_log_enabled {
                    self.trace_log.log_event(
                        &self.ctx,
                        json!({
                            "time": self.ctx.time(),
                            "type": "task_scheduled",
                            "task_id": task_id,
                            "task_name": task.name.clone(),
                            "location": resource.name.clone(),
                            "cores": need_cores,
                            "memory": task.memory,
                        }),
                    );
                }
                self.dag.update_task_state(task_id, TaskState::Running);

                if self.task_inputs.get(&task_id).unwrap().is_empty() {
                    self.start_task(task_id);
                }

                something_scheduled = true;
                self.scheduled_actions.insert(action_id);
            }

            if !something_scheduled {
                break;
            }
        }
    }

    fn on_task_completed(&mut self, task_id: usize) {
        let task_name = self.dag.get_task(task_id).name.clone();
        if self.trace_log_enabled {
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "task_completed",
                    "task_id": task_id,
                    "task_name": task_name,
                }),
            );
        }
        let location = self.task_location.remove(&task_id).unwrap();
        let task_cores = self.task_cores.get(&task_id).unwrap();
        self.resources[location].cores_available += task_cores.len() as u32;
        for &core in task_cores.iter() {
            self.available_cores[location].insert(core);
        }
        self.resources[location].memory_available += self.dag.get_task(task_id).memory;
        let data_items = self.dag.update_task_state(task_id, TaskState::Done);

        for &data_item_id in data_items.iter() {
            if self.config.data_transfer_mode == DataTransferMode::Direct && !self.outputs.contains(&data_item_id) {
                // upload to runner only DAG outputs
                continue;
            }

            let data_item = self.dag.get_data_item(data_item_id);
            let data_id = self.network.borrow_mut().transfer_data(
                self.resources[location].id,
                self.id,
                data_item.size as f64,
                self.id,
            );
            self.data_transfers.insert(
                data_id,
                DataTransfer {
                    data_id: data_item_id,
                    task_id,
                    from: self.resources[location].id,
                    to: self.id,
                },
            );
            if self.trace_log_enabled {
                self.trace_log.log_event(
                    &self.ctx,
                    json!({
                        "time": self.ctx.time(),
                        "type": "start_uploading",
                        "from": self.resources[location].name.clone(),
                        "to": "scheduler",
                        "data_id": data_id,
                        "data_name": data_item.name.clone(),
                        "task_id": task_id,
                    }),
                );
            }
        }

        if self.config.data_transfer_mode == DataTransferMode::Direct {
            for &data_item_id in data_items.iter() {
                self.data_location.insert(data_item_id, self.resources[location].id);
                self.on_data_item_is_ready(data_item_id);
            }
        }

        self.actions.extend(self.scheduler.borrow_mut().on_task_state_changed(
            task_id,
            TaskState::Done,
            &self.dag,
            &self.resources,
            &self.ctx,
        ));
        self.process_actions();

        self.check_and_log_completed();
    }

    fn start_task(&mut self, task_id: usize) {
        let task = self.dag.get_task(task_id);
        let location = *self.task_location.get(&task_id).unwrap();
        let cores = self.task_cores.get(&task_id).unwrap().len() as u32;
        let computation_id = self.resources[location].compute.borrow_mut().run(
            task.flops,
            task.memory,
            cores,
            cores,
            task.cores_dependency,
            self.id,
        );
        self.computations.insert(computation_id, task_id);

        if self.trace_log_enabled {
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "task_started",
                    "task_id": task_id,
                    "task_name": task.name.clone(),
                }),
            );
        }
    }

    fn on_data_transfered(&mut self, data_event_id: usize) {
        let data_transfer = self.data_transfers.remove(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);
        let task_id = data_transfer.task_id;
        if data_transfer.to == self.id {
            // uploaded data to runner

            if self.trace_log_enabled {
                self.trace_log.log_event(
                    &self.ctx,
                    json!({
                        "time": self.ctx.time(),
                        "type": "finish_uploading",
                        "from": self.ctx.lookup_name(data_transfer.from.clone()),
                        "to": "scheduler",
                        "data_id": data_event_id,
                        "data_name": data_item.name.clone(),
                        "task_id": task_id,
                    }),
                );
            }
            self.data_location.insert(data_id, self.id);
            self.on_data_item_is_ready(data_id);
        } else {
            // downloaded data from runner or another resource

            let location = *self.task_location.get(&task_id).unwrap();
            if self.trace_log_enabled {
                self.trace_log.log_event(
                    &self.ctx,
                    json!({
                        "time": self.ctx.time(),
                        "type": "finish_uploading",
                        "from": self.ctx.lookup_name(data_transfer.from.clone()),
                        "to": self.resources[location].name.clone(),
                        "data_id": data_event_id,
                        "data_name": data_item.name.clone(),
                        "task_id": task_id,
                    }),
                );
            }

            let left_inputs = self.task_inputs.get_mut(&task_id).unwrap();
            left_inputs.remove(&data_id);
            if left_inputs.is_empty() {
                self.start_task(task_id);
            }
        }

        self.check_and_log_completed();
    }

    fn check_and_log_completed(&self) {
        if self.dag.is_completed() && self.data_transfers.is_empty() {
            log_info!(self.ctx, "finished DAG execution");
        }
    }

    pub fn validate_completed(&self) {
        if !self.dag.is_completed() {
            let mut states: Vec<String> = Vec::new();
            for task_state in TaskState::into_enum_iter() {
                let cnt = self
                    .dag
                    .get_tasks()
                    .iter()
                    .filter(|task| task.state == task_state)
                    .count();
                if cnt != 0 {
                    states.push(format!("{} {:?}", cnt, task_state));
                }
            }
            log_error!(self.ctx, "DAG is not completed, currently {} tasks", states.join(", "));
        }
    }

    fn on_data_item_is_ready(&mut self, data_id: usize) {
        for (task, state) in self
            .dag
            .update_data_item_state(data_id, DataItemState::Ready)
            .into_iter()
        {
            self.actions.extend(self.scheduler.borrow_mut().on_task_state_changed(
                task,
                state,
                &self.dag,
                &self.resources,
                &self.ctx,
            ));
            self.process_actions();
        }
    }
}

#[derive(Serialize)]
pub struct Start {}

impl EventHandler for DAGRunner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.start()
            }
            CompStarted { .. } => {}
            CompFinished { id } => {
                let task_id = self.computations.remove(&id).unwrap();
                self.on_task_completed(task_id);
            }
            DataTransferCompleted { data } => {
                self.on_data_transfered(data.id);
            }
        })
    }
}
