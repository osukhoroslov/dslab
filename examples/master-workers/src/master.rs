use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use serde::Serialize;

use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::{cast, log_debug, log_info, log_trace};
use network::network::Network;

use crate::common::Start;
use crate::task::*;
use crate::worker::{TaskCompleted, WorkerRegister};

#[derive(Serialize)]
pub struct ReportStatus {}

#[derive(Serialize)]
pub struct ScheduleTasks {}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum WorkerState {
    Online,
    Offline,
}

#[derive(Debug)]
pub struct WorkerInfo {
    id: u32,
    state: WorkerState,
    speed: u64,
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
}

pub struct Master {
    id: u32,
    net: Rc<RefCell<Network>>,
    workers: BTreeMap<u32, WorkerInfo>,
    unassigned_tasks: BTreeMap<u64, TaskInfo>,
    assigned_tasks: HashMap<u64, TaskInfo>,
    cpus_available: u32,
    memory_available: u64,
    completed: bool,
    worker_speed: Vec<(u32, u64)>,
    ctx: SimulationContext,
}

impl Master {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            workers: BTreeMap::new(),
            unassigned_tasks: BTreeMap::new(),
            assigned_tasks: HashMap::new(),
            cpus_available: 0,
            memory_available: 0,
            completed: false,
            worker_speed: Vec::new(),
            ctx,
        }
    }

    pub fn schedule_tasks(&mut self) {
        log_trace!(self.ctx, "scheduling tasks");
        let mut assigned_tasks = HashSet::new();
        let mut min_cores = u32::MAX;
        let mut min_memory = u64::MAX;
        for (task_id, task) in self.unassigned_tasks.iter_mut() {
            if task.req.min_cores > self.cpus_available || task.req.memory > self.memory_available {
                continue;
            }
            if task.req.min_cores > min_cores && task.req.memory > min_memory {
                continue;
            }
            let mut assigned = false;
            for (worker_id, _) in &self.worker_speed {
                let worker = self.workers.get_mut(worker_id).unwrap();
                if worker.state == WorkerState::Online
                    && worker.cpus_available >= task.req.min_cores
                    && worker.memory_available >= task.req.memory
                {
                    log_debug!(self.ctx, "assigned task {} to worker {}", task_id, worker_id);
                    task.state = TaskState::Assigned;
                    assigned_tasks.insert(*task_id);
                    worker.cpus_available -= task.req.min_cores;
                    worker.memory_available -= task.req.memory;
                    self.cpus_available -= task.req.min_cores;
                    self.memory_available -= task.req.memory;
                    self.net.borrow_mut().send_event(task.req.clone(), self.id, *worker_id);
                    assigned = true;
                    break;
                }
            }
            if !assigned && task.req.min_cores <= min_cores && task.req.memory <= min_memory {
                min_cores = task.req.min_cores;
                min_memory = task.req.memory;
            }
            // time optimization!
            if !assigned {
                break;
            }
        }
        for task_id in assigned_tasks.iter() {
            let task = self.unassigned_tasks.remove(task_id).unwrap();
            self.assigned_tasks.insert(*task_id, task);
        }
    }

    pub fn schedule_on_worker(&mut self, worker_id: u32) {
        log_trace!(self.ctx, "scheduling tasks on worker {}", worker_id);
        let worker = self.workers.get_mut(&worker_id).unwrap();
        let mut assigned_tasks = HashSet::new();
        for (task_id, task) in self.unassigned_tasks.iter_mut() {
            if worker.state == WorkerState::Online
                && worker.cpus_available >= task.req.min_cores
                && worker.memory_available >= task.req.memory
            {
                log_debug!(self.ctx, "assigned task {} to worker {}", task_id, worker.id);
                task.state = TaskState::Assigned;
                assigned_tasks.insert(*task_id);
                worker.cpus_available -= task.req.min_cores;
                worker.memory_available -= task.req.memory;
                self.cpus_available -= task.req.min_cores;
                self.memory_available -= task.req.memory;
                self.net.borrow_mut().send_event(task.req.clone(), self.id, worker.id);
                // time optimization!
                if assigned_tasks.len() == 1 {
                    break;
                }
            }
        }
        for task_id in assigned_tasks.iter() {
            let task = self.unassigned_tasks.remove(task_id).unwrap();
            self.assigned_tasks.insert(*task_id, task);
        }
    }
}

impl EventHandler for Master {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                log_debug!(self.ctx, "started");
                self.ctx.emit_self(ScheduleTasks {}, 5.);
            }
            ScheduleTasks {} => {
                self.schedule_tasks();
                // if !self.completed {
                //     self.ctx.emit_self(ScheduleTasks {}, 5.);
                // }
            }
            WorkerRegister {
                speed,
                cpus_total,
                memory_total,
            } => {
                let worker = WorkerInfo {
                    id: event.src,
                    state: WorkerState::Online,
                    speed,
                    cpus_total,
                    cpus_available: cpus_total,
                    memory_total,
                    memory_available: memory_total,
                };
                log_debug!(self.ctx, "registered worker: {:?}", worker);
                self.cpus_available += worker.cpus_available;
                self.memory_available += worker.memory_available;
                self.workers.insert(worker.id.clone(), worker);
                // sort workers by speed
                self.worker_speed = Vec::from_iter(self.workers.iter().map(|(w_id, w)| (w_id.clone(), w.speed)));
                self.worker_speed
                    .sort_by(|(id1, s1), (id2, s2)| s1.cmp(s2).reverse().then(id1.cmp(&id2)));
            }
            TaskRequest {
                id,
                flops,
                memory,
                min_cores,
                max_cores,
                cores_dependency,
                input_size,
                output_size,
            } => {
                let task = TaskInfo {
                    req: TaskRequest {
                        id,
                        flops,
                        memory,
                        min_cores,
                        max_cores,
                        cores_dependency,
                        input_size,
                        output_size,
                    },
                    state: TaskState::New,
                };
                log_debug!(self.ctx, "task request: {:?}", task.req);
                self.unassigned_tasks.insert(task.req.id, task);
            }
            TaskCompleted { id } => {
                log_debug!(self.ctx, "completed task: {:?}", id);
                let task = self.assigned_tasks.get_mut(&id).unwrap();
                task.state = TaskState::Completed;
                let worker = self.workers.get_mut(&event.src).unwrap();
                worker.cpus_available += task.req.min_cores;
                worker.memory_available += task.req.memory;
                self.cpus_available += task.req.min_cores;
                self.memory_available += task.req.memory;
                self.schedule_on_worker(event.src);
            }
            ReportStatus {} => {
                log_info!(self.ctx, "workers: {}", self.workers.len());
                let total_cpus: u64 = self.workers.values().map(|w| w.cpus_total as u64).sum();
                let available_cpus: u64 = self.workers.values().map(|w| w.cpus_available as u64).sum();
                let total_memory: u64 = self.workers.values().map(|w| w.memory_total as u64).sum();
                let available_memory: u64 = self.workers.values().map(|w| w.memory_available as u64).sum();
                log_info!(
                    self.ctx,
                    "--- cpus: {} / {}, memory: {} / {}",
                    available_cpus,
                    total_cpus,
                    available_memory,
                    total_memory,
                );
                let task_count = self.unassigned_tasks.len() + self.assigned_tasks.len();
                let completed_count = self
                    .assigned_tasks
                    .values()
                    .filter(|t| matches!(t.state, TaskState::Completed))
                    .count();
                log_info!(self.ctx, "--- tasks: {} / {}", completed_count, task_count);
                if task_count == 0 || completed_count != task_count {
                    self.ctx.emit_self(ReportStatus {}, 10.);
                } else {
                    self.completed = true
                }
            }
        })
    }
}
