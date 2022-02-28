use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use log::{debug, info, trace};

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use network::network::Network;

use crate::common::Start;
use crate::task::*;
use crate::worker::{TaskCompleted, WorkerRegister};

#[derive(Debug)]
pub struct ReportStatus {}

#[derive(Debug)]
pub struct ScheduleTasks {}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum WorkerState {
    Online,
    Offline,
}

#[derive(Debug)]
pub struct WorkerInfo {
    id: String,
    state: WorkerState,
    speed: u64,
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
}

pub struct Master {
    id: String,
    net: Rc<RefCell<Network>>,
    workers: BTreeMap<String, WorkerInfo>,
    tasks: BTreeMap<u64, TaskInfo>,
    cpus_available: u32,
    memory_available: u64,
    completed: bool,
    worker_speed: Vec<(String, u64)>,
    ctx: SimulationContext,
}

impl Master {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id().to_string(),
            net,
            workers: BTreeMap::new(),
            tasks: BTreeMap::new(),
            cpus_available: 0,
            memory_available: 0,
            completed: false,
            worker_speed: Vec::new(),
            ctx,
        }
    }

    pub fn schedule_tasks(&mut self) {
        trace!("{} [{}] scheduling tasks", self.ctx.time(), self.id);
        for (task_id, task) in self.tasks.iter_mut() {
            if matches!(task.state, TaskState::New) {
                let mut assigned = false;
                if self.cpus_available >= task.req.min_cores && self.memory_available >= task.req.memory {
                    for (worker_id, _) in &self.worker_speed {
                        let worker = self.workers.get_mut(worker_id).unwrap();
                        if worker.state == WorkerState::Online
                            && worker.cpus_available >= task.req.min_cores
                            && worker.memory_available >= task.req.memory
                        {
                            debug!(
                                "{} [{}] - assigned task {} to worker {}",
                                self.ctx.time(),
                                self.id,
                                task_id,
                                worker_id
                            );
                            task.state = TaskState::Assigned;
                            worker.cpus_available -= task.req.min_cores;
                            worker.memory_available -= task.req.memory;
                            self.cpus_available -= task.req.min_cores;
                            self.memory_available -= task.req.memory;
                            self.net.borrow_mut().send_event(task.req.clone(), &self.id, &worker_id);
                            assigned = true;
                            break;
                        }
                    }
                }
                if !assigned {
                    break;
                }
            }
        }
    }
}

impl EventHandler for Master {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                debug!("{} [{}] started", event.time, self.id);
                self.ctx.emit_self(ScheduleTasks {}, 5.);
            }
            ScheduleTasks {} => {
                self.schedule_tasks();
                if !self.completed {
                    self.ctx.emit_self(ScheduleTasks {}, 5.);
                }
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
                debug!("{} [{}] registered worker: {:?}", event.time, self.id, worker);
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
                debug!("{} [{}] task request: {:?}", event.time, self.id, task.req);
                self.tasks.insert(task.req.id, task);
            }
            TaskCompleted { id } => {
                debug!("{} [{}] completed task: {:?}", event.time, self.id, id);
                let task = self.tasks.get_mut(&id).unwrap();
                task.state = TaskState::Completed;
                let worker = self.workers.get_mut(&event.src).unwrap();
                worker.cpus_available += task.req.min_cores;
                worker.memory_available += task.req.memory;
                self.cpus_available += task.req.min_cores;
                self.memory_available += task.req.memory;
                self.schedule_tasks();
            }
            ReportStatus {} => {
                info!("{} [{}] workers: {}", event.time, self.id, self.workers.len());
                let total_cpus: u64 = self.workers.values().map(|w| w.cpus_total as u64).sum();
                let available_cpus: u64 = self.workers.values().map(|w| w.cpus_available as u64).sum();
                let total_memory: u64 = self.workers.values().map(|w| w.memory_total as u64).sum();
                let available_memory: u64 = self.workers.values().map(|w| w.memory_available as u64).sum();
                info!(
                    "{} [{}] --- cpus: {} / {}, memory: {} / {}",
                    event.time, self.id, available_cpus, total_cpus, available_memory, total_memory,
                );
                let task_count = self.tasks.len();
                let completed_count = self
                    .tasks
                    .values()
                    .filter(|t| matches!(t.state, TaskState::Completed))
                    .count();
                info!(
                    "{} [{}] --- tasks: {} / {}",
                    event.time, self.id, completed_count, task_count
                );
                if task_count == 0 || completed_count != task_count {
                    self.ctx.emit_self(ReportStatus {}, 10.);
                } else {
                    self.completed = true
                }
            }
        })
    }
}
