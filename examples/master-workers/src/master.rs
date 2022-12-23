use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

use log::log_enabled;
use log::Level::Info;
use priority_queue::PriorityQueue;
use serde::Serialize;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, log_info, log_trace};
use dslab_network::network::Network;

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
    id: Id,
    #[allow(dead_code)]
    state: WorkerState,
    speed: u64,
    #[allow(dead_code)]
    cpus_total: u32,
    cpus_available: u32,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
}

type WorkerScore = (u64, u32, u64);

impl WorkerInfo {
    pub fn score(&self) -> WorkerScore {
        (self.memory_available, self.cpus_available, self.speed)
    }
}

pub struct Master {
    id: Id,
    net: Rc<RefCell<Network>>,
    workers: BTreeMap<Id, WorkerInfo>,
    worker_queue: PriorityQueue<Id, WorkerScore>,
    unassigned_tasks: BTreeMap<u64, TaskInfo>,
    assigned_tasks: HashMap<u64, TaskInfo>,
    completed_tasks: HashMap<u64, TaskInfo>,
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
    pub scheduling_time: f64,
    ctx: SimulationContext,
}

impl Master {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            workers: BTreeMap::new(),
            worker_queue: PriorityQueue::new(),
            unassigned_tasks: BTreeMap::new(),
            assigned_tasks: HashMap::new(),
            completed_tasks: HashMap::new(),
            cpus_total: 0,
            cpus_available: 0,
            memory_total: 0,
            memory_available: 0,
            scheduling_time: 0.,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit_self(ScheduleTasks {}, 1.);
        if log_enabled!(Info) {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn on_worker_register(&mut self, worker_id: Id, cpus_total: u32, memory_total: u64, speed: u64) {
        let worker = WorkerInfo {
            id: worker_id,
            state: WorkerState::Online,
            speed,
            cpus_total,
            cpus_available: cpus_total,
            memory_total,
            memory_available: memory_total,
        };
        log_debug!(self.ctx, "registered worker: {:?}", worker);
        self.cpus_total += worker.cpus_total;
        self.cpus_available += worker.cpus_available;
        self.memory_total += worker.memory_total;
        self.memory_available += worker.memory_available;
        self.worker_queue.push(worker.id, worker.score());
        self.workers.insert(worker.id, worker);
    }

    fn on_task_request(&mut self, req: TaskRequest) {
        let task = TaskInfo {
            req,
            state: TaskState::New,
        };
        log_debug!(self.ctx, "task request: {:?}", task.req);
        self.unassigned_tasks.insert(task.req.id, task);
    }

    fn on_task_completed(&mut self, task_id: u64, worker_id: Id) {
        log_debug!(self.ctx, "completed task: {:?}", task_id);
        let mut task = self.assigned_tasks.remove(&task_id).unwrap();
        task.state = TaskState::Completed;
        let worker = self.workers.get_mut(&worker_id).unwrap();
        worker.cpus_available += task.req.min_cores;
        worker.memory_available += task.req.memory;
        self.cpus_available += task.req.min_cores;
        self.memory_available += task.req.memory;
        self.worker_queue.push(worker.id, worker.score());
        self.completed_tasks.insert(task_id, task);
        if self.assigned_tasks.is_empty() {
            self.schedule_tasks();
        }
    }

    fn schedule_tasks(&mut self) {
        if self.unassigned_tasks.is_empty() {
            return;
        }
        log_trace!(self.ctx, "scheduling tasks");
        let t = Instant::now();
        let mut assigned_tasks = HashSet::new();
        for (task_id, task) in self.unassigned_tasks.iter_mut() {
            if self.worker_queue.is_empty() {
                break;
            }
            if task.req.min_cores > self.cpus_available || task.req.memory > self.memory_available {
                continue;
            }
            let mut checked_workers = Vec::new();
            while let Some((worker_id, (memory, cpus, speed))) = self.worker_queue.pop() {
                if cpus >= task.req.min_cores && memory >= task.req.memory {
                    log_debug!(self.ctx, "assigned task {} to worker {}", task_id, worker_id);
                    task.state = TaskState::Assigned;
                    assigned_tasks.insert(*task_id);
                    let worker = self.workers.get_mut(&worker_id).unwrap();
                    worker.cpus_available -= task.req.min_cores;
                    worker.memory_available -= task.req.memory;
                    self.cpus_available -= task.req.min_cores;
                    self.memory_available -= task.req.memory;
                    checked_workers.push((worker.id, worker.score()));
                    self.net.borrow_mut().send_event(task.req.clone(), self.id, worker_id);
                    break;
                } else {
                    checked_workers.push((worker_id, (memory, cpus, speed)));
                }
                if memory <= task.req.memory {
                    break;
                }
            }
            for (worker_id, (memory, cpus, speed)) in checked_workers.into_iter() {
                if memory > 0 && cpus > 0 {
                    self.worker_queue.push(worker_id, (memory, cpus, speed));
                }
            }
        }
        let assigned_count = assigned_tasks.len();
        for task_id in assigned_tasks.into_iter() {
            let task = self.unassigned_tasks.remove(&task_id).unwrap();
            self.assigned_tasks.insert(task_id, task);
        }
        let schedule_duration = t.elapsed();
        log_info!(
            self.ctx,
            "schedule_tasks: assigned {} tasks in {:.2?}",
            assigned_count,
            schedule_duration
        );
        self.scheduling_time += schedule_duration.as_secs_f64();
        if self.is_active() {
            self.ctx.emit_self(ScheduleTasks {}, 10.);
        }
    }

    fn report_status(&mut self) {
        log_info!(
            self.ctx,
            "CPU: {:.2} / MEMORY: {:.2} / UNASSIGNED: {} / ASSIGNED: {} / COMPLETED: {}",
            (self.cpus_total - self.cpus_available) as f64 / self.cpus_total as f64,
            (self.memory_total - self.memory_available) as f64 / self.memory_total as f64,
            self.unassigned_tasks.len(),
            self.assigned_tasks.len(),
            self.completed_tasks.len()
        );
        if self.is_active() {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn is_active(&self) -> bool {
        !self.unassigned_tasks.is_empty() || !self.assigned_tasks.is_empty()
    }
}

impl EventHandler for Master {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_started();
            }
            ScheduleTasks {} => {
                self.schedule_tasks();
            }
            WorkerRegister {
                speed,
                cpus_total,
                memory_total,
            } => {
                self.on_worker_register(event.src, cpus_total, memory_total, speed);
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
                self.on_task_request(TaskRequest {
                    id,
                    flops,
                    memory,
                    min_cores,
                    max_cores,
                    cores_dependency,
                    input_size,
                    output_size,
                });
            }
            TaskCompleted { id } => {
                self.on_task_completed(id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
        })
    }
}
