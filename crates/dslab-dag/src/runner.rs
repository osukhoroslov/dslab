//! DAG execution runtime.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde::Serialize;
use serde_json::json;

use enum_iterator::IntoEnumIterator;

use dslab_compute::multicore::*;
use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{log_debug, log_error, log_info};
use dslab_network::model::DataTransferCompleted;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::data_item::{DataItemState, DataTransferMode};
use crate::resource::Resource;
use crate::scheduler::{Action, Scheduler, TimeSpan};
use crate::system::System;
use crate::task::TaskState;
use crate::trace_log::TraceLog;

/// Represents a DAG execution configuration.
#[derive(Clone)]
pub struct Config {
    pub data_transfer_mode: DataTransferMode,
}

/// Represents a transfer of data item between resources.
struct DataTransfer {
    data_id: usize,
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

/// Manages the execution of a DAG on a specified set of computing resources.
///
/// Invokes the supplied scheduler and executes the actions returned by the scheduler.
/// Tracks and updates the states of DAG tasks, data items and data transfers.
/// Supports collection of DAG execution log.
pub struct DAGRunner {
    id: Id,
    dag: DAG,
    network: Rc<RefCell<Network>>,
    resources: Vec<Resource>,
    resource_indexes: HashMap<Id, usize>,
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
    // data_transfer_tasks[x][y] -- set of actors where we should send data_item y from actor x
    data_transfer_tasks: HashMap<Id, HashMap<usize, Vec<Id>>>,
    resource_data_items: HashMap<Id, BTreeSet<usize>>,
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
        let resource_indexes = resources
            .iter()
            .enumerate()
            .map(|(idx, resource)| (resource.id, idx))
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
            resource_indexes,
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
            data_transfer_tasks: HashMap::new(),
            resource_data_items: HashMap::new(),
            available_cores,
            trace_log_enabled: true,
            config,
            ctx,
        }
    }

    /// Enables or disables [trace log](crate::trace_log::TraceLog).
    pub fn enable_trace_log(&mut self, flag: bool) {
        self.trace_log_enabled = flag;
    }

    /// Starts DAG execution.
    pub fn start(&mut self) {
        if !self.validate_input() {
            return;
        }

        for (id, data_item) in self.dag.get_data_items().iter().enumerate() {
            if data_item.state == DataItemState::Ready {
                assert!(data_item.producer.is_none(), "Non-input data item has Ready state");
                self.data_location.insert(id, self.id);
                self.resource_data_items.entry(self.id).or_default().insert(id);
            } else if data_item.consumers.is_empty() {
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
        let actions = self.scheduler.borrow_mut().start(
            &self.dag,
            System {
                resources: &self.resources,
                network: &self.network.borrow(),
            },
            self.config.clone(),
            &self.ctx,
        );
        if let Some(makespan) = actions
            .iter()
            .filter_map(|action| match action {
                Action::ScheduleTask {
                    expected_span,
                    task,
                    resource,
                    ..
                }
                | Action::ScheduleTaskOnCores {
                    expected_span,
                    task,
                    resource,
                    ..
                } => expected_span.as_ref().map(|x| {
                    x.finish()
                        + self
                            .dag
                            .get_task(*task)
                            .outputs
                            .iter()
                            .filter(|f| self.dag.get_outputs().contains(f))
                            .map(|&f| {
                                self.dag.get_data_item(f).size as f64
                                    / self
                                        .network
                                        .borrow()
                                        .bandwidth(self.resources[*resource].id, self.ctx.id())
                            })
                            .max_by(|a, b| a.total_cmp(&b))
                            .unwrap_or(0.)
                }),
                Action::TransferData { .. } => None,
            })
            .max_by(|a, b| a.total_cmp(&b))
        {
            log_info!(self.ctx, "expected makespan: {}", makespan);
        }
        self.actions.extend(actions);
        self.process_actions();
    }

    fn validate_input(&self) -> bool {
        if self.dag.get_tasks().iter().map(|task| task.min_cores).max()
            > self.resources.iter().map(|r| r.compute.borrow().cores_total()).max()
        {
            log_error!(self.ctx, "some tasks require more cores than any resource can provide");
            return false;
        }
        true
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

    /// Returns trace log.
    pub fn trace_log(&self) -> &TraceLog {
        &self.trace_log
    }

    fn process_schedule_action(
        &mut self,
        task: usize,
        resource: usize,
        need_cores: u32,
        allowed_cores: Vec<u32>,
        expected_span: Option<TimeSpan>,
    ) {
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
        let data_items = self.dag.get_task(task_id).inputs.clone();
        self.task_location.insert(task_id, resource);
        if self.config.data_transfer_mode == DataTransferMode::ViaMasterNode {
            for &data_item_id in data_items.iter() {
                self.add_data_transfer_task(data_item_id, self.id, self.resources[resource].id);
            }
        } else if self.config.data_transfer_mode == DataTransferMode::Direct {
            for &data_item_id in data_items.iter() {
                if let Some(location) = self.data_location.get(&data_item_id).cloned() {
                    if location != self.resources[resource].id {
                        self.add_data_transfer_task(data_item_id, location, self.resources[resource].id);
                    }
                }
            }
        }
        for core in allowed_cores.into_iter() {
            self.resource_queue[resource][core as usize].push_back(QueuedTask {
                task_id,
                cores: need_cores,
                action_id: self.action_id,
            });
        }
        if let Some(time_span) = expected_span {
            log_debug!(
                self.ctx,
                "Expected span for task {} is {} - {}",
                task_id,
                time_span.start(),
                time_span.finish()
            );
        }
        self.process_resource_queue(resource);
    }

    fn process_actions(&mut self) {
        for i in 0..self.resources.len() {
            self.process_resource_queue(i);
        }
        while let Some(action) = self.actions.pop_front() {
            log_debug!(self.ctx, "Got action: {:?}", action);
            match action {
                Action::ScheduleTask {
                    task,
                    resource,
                    cores,
                    expected_span,
                } => {
                    let allowed_cores =
                        (0..self.resources[resource].compute.borrow().cores_total()).collect::<Vec<_>>();
                    self.process_schedule_action(task, resource, cores, allowed_cores, expected_span);
                }
                Action::ScheduleTaskOnCores {
                    task,
                    resource,
                    mut cores,
                    expected_span,
                } => {
                    cores.sort();
                    if cores.windows(2).any(|window| window[0] == window[1]) {
                        log_error!(self.ctx, "Wrong action, cores list {:?} contains same cores", cores);
                        return;
                    }
                    self.process_schedule_action(task, resource, cores.len() as u32, cores, expected_span);
                }
                Action::TransferData { data_item, from, to } => {
                    self.add_data_transfer_task(data_item, from, to);
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
                if !task.inputs.iter().all(|x| {
                    self.resource_data_items
                        .entry(self.resources[resource_idx].id)
                        .or_default()
                        .contains(x)
                }) {
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

                self.start_task(task_id);

                something_scheduled = true;
                self.scheduled_actions.insert(action_id);
            }

            if !something_scheduled {
                break;
            }
        }
    }

    fn transfer_data(&mut self, data_item_id: usize, from: Id, to: Id) {
        let data_item = self.dag.get_data_item(data_item_id);
        let data_id = self
            .network
            .borrow_mut()
            .transfer_data(from, to, data_item.size as f64, self.id);
        self.data_transfers.insert(
            data_id,
            DataTransfer {
                data_id: data_item_id,
                from,
                to,
            },
        );
        if self.trace_log_enabled {
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "start_uploading",
                    "from": self.ctx.lookup_name(from),
                    "to": self.ctx.lookup_name(to),
                    "data_id": data_id,
                    "data_item_id": data_item_id,
                    "data_name": data_item.name.clone(),
                }),
            );
        }
    }

    fn add_data_transfer_task(&mut self, data_item_id: usize, from: Id, to: Id) {
        if self
            .resource_data_items
            .entry(from)
            .or_default()
            .contains(&data_item_id)
        {
            self.transfer_data(data_item_id, from, to);
        } else {
            self.data_transfer_tasks
                .entry(from)
                .or_default()
                .entry(data_item_id)
                .or_default()
                .push(to);
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
        let location = *self.task_location.get(&task_id).unwrap();
        let task_cores = self.task_cores.get(&task_id).unwrap();
        self.resources[location].cores_available += task_cores.len() as u32;
        for &core in task_cores.iter() {
            self.available_cores[location].insert(core);
        }
        self.resources[location].memory_available += self.dag.get_task(task_id).memory;
        self.dag.update_task_state(task_id, TaskState::Done);
        let data_items = self.dag.get_task(task_id).outputs.clone();

        if self.config.data_transfer_mode != DataTransferMode::ViaMasterNode {
            for &data_item_id in data_items.iter() {
                self.resource_data_items
                    .entry(self.resources[location].id)
                    .or_default()
                    .insert(data_item_id);

                if let Some(targets) = self
                    .data_transfer_tasks
                    .entry(self.resources[location].id)
                    .or_default()
                    .remove(&data_item_id)
                {
                    for target in targets.into_iter() {
                        self.transfer_data(data_item_id, self.resources[location].id, target);
                    }
                }
            }
        }

        if self.config.data_transfer_mode == DataTransferMode::Direct {
            for &data_item_id in data_items.iter() {
                self.data_location.insert(data_item_id, self.resources[location].id);
            }

            for &data_item_id in data_items.iter() {
                for consumer in self.dag.get_data_item(data_item_id).consumers.clone().iter() {
                    if let Some(consumer_location) = self.task_location.get(&consumer).cloned() {
                        if location != consumer_location {
                            self.add_data_transfer_task(
                                data_item_id,
                                self.resources[location].id,
                                self.resources[consumer_location].id,
                            );
                        }
                    }
                }
            }
        }

        if self.config.data_transfer_mode != DataTransferMode::Manual {
            for &data_item_id in data_items.iter() {
                if self.config.data_transfer_mode == DataTransferMode::Direct && !self.outputs.contains(&data_item_id) {
                    // upload to runner only DAG outputs
                    continue;
                }

                self.transfer_data(data_item_id, self.resources[location].id, self.id);
            }
        }

        if !self.scheduler.borrow().is_static() {
            self.actions.extend(self.scheduler.borrow_mut().on_task_state_changed(
                task_id,
                TaskState::Done,
                &self.dag,
                System {
                    resources: &self.resources,
                    network: &self.network.borrow(),
                },
                &self.ctx,
            ));
        }
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

    fn on_data_transfer_completed(&mut self, data_event_id: usize) {
        let data_transfer = self.data_transfers.remove(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);

        if self.trace_log_enabled {
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "finish_uploading",
                    "from": self.ctx.lookup_name(data_transfer.from.clone()),
                    "to": self.ctx.lookup_name(data_transfer.to),
                    "data_id": data_event_id,
                    "data_name": data_item.name.clone(),
                }),
            );
        }

        self.resource_data_items
            .entry(data_transfer.to)
            .or_default()
            .insert(data_id);

        if let Some(targets) = self
            .data_transfer_tasks
            .entry(data_transfer.to)
            .or_default()
            .remove(&data_id)
        {
            for target in targets.into_iter() {
                self.transfer_data(data_id, data_transfer.to, target);
            }
        }

        if let Some(resource_idx) = self.resource_indexes.get(&data_transfer.to).cloned() {
            self.process_resource_queue(resource_idx);
        }

        self.check_and_log_completed();
    }

    pub fn is_completed(&self) -> bool {
        self.dag.is_completed() && self.data_transfers.is_empty()
    }

    fn check_and_log_completed(&self) {
        if self.is_completed() {
            log_info!(self.ctx, "finished DAG execution");
        }
    }

    /// Checks that all DAG tasks are completed.
    pub fn validate_completed(&self) {
        if !self.is_completed() {
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
                self.on_data_transfer_completed(data.id);
            }
        })
    }
}
