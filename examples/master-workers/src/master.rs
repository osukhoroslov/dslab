use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::common::Start;
use crate::network::*;
use crate::task::*;
use crate::worker::{TaskCompleted, WorkerRegister};
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

#[derive(Debug)]
pub struct ReportStatus {}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum WorkerState {
    Online,
    Offline,
}

#[derive(Debug)]
pub struct WorkerInfo {
    id: ActorId,
    state: WorkerState,
    speed: u64,
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
}

pub struct Master {
    net: Rc<RefCell<Network>>,
    workers: BTreeMap<ActorId, WorkerInfo>,
    tasks: BTreeMap<u64, TaskInfo>,
}

impl Master {
    pub fn new(net: Rc<RefCell<Network>>) -> Self {
        Self {
            net,
            workers: BTreeMap::new(),
            tasks: BTreeMap::new(),
        }
    }

    pub fn schedule_tasks(&mut self, ctx: &mut ActorContext) {
        println!("{} [{}] scheduling tasks", ctx.time(), ctx.id);
        for (task_id, task) in self.tasks.iter_mut() {
            if matches!(task.state, TaskState::New) {
                // sort workers by speed
                let mut worker_speed = Vec::from_iter(self.workers.iter().map(|(w_id, w)| (w_id.clone(), w.speed)));
                worker_speed.sort_by(|(id1, s1), (id2, s2)| s1.cmp(s2).reverse().then(id1.cmp(&id2)));
                for (worker_id, _) in worker_speed {
                    let worker = self.workers.get_mut(&worker_id).unwrap();
                    if worker.state == WorkerState::Online
                        && worker.cpus_available >= task.req.min_cores
                        && worker.memory_available >= task.req.memory
                    {
                        println!(
                            "{} [{}] - assigned task {} to worker {}",
                            ctx.time(),
                            ctx.id,
                            task_id,
                            worker_id
                        );
                        task.state = TaskState::Assigned;
                        worker.cpus_available -= task.req.min_cores;
                        worker.memory_available -= task.req.memory;
                        self.net.borrow().send(task.req, worker_id.clone(), ctx);
                        break;
                    }
                }
            }
        }
    }
}

impl Actor for Master {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
            }
            WorkerRegister {
                speed,
                cpus_total,
                memory_total,
            } => {
                let worker = WorkerInfo {
                    id: from,
                    state: WorkerState::Online,
                    speed: *speed,
                    cpus_total: *cpus_total,
                    cpus_available: *cpus_total,
                    memory_total: *memory_total,
                    memory_available: *memory_total,
                };
                println!("{} [{}] registered worker: {:?}", ctx.time(), ctx.id, worker);
                self.workers.insert(worker.id.clone(), worker);
            }
            TaskRequest { .. } => {
                println!("{} [{}] task request: {:?}", ctx.time(), ctx.id, event);
                let task = TaskInfo {
                    req: *event.downcast::<TaskRequest>().unwrap(),
                    state: TaskState::New,
                };
                self.tasks.insert(task.req.id, task);
                self.schedule_tasks(ctx);
            }
            TaskCompleted { id } => {
                println!("{} [{}] completed task: {:?}", ctx.time(), ctx.id, id);
                let task = self.tasks.get_mut(id).unwrap();
                task.state = TaskState::Completed;
                let worker = self.workers.get_mut(&from).unwrap();
                worker.cpus_available += task.req.min_cores;
                worker.memory_available += task.req.memory;
                self.schedule_tasks(ctx);
            }
            ReportStatus {} => {
                println!("{} [{}] workers: {}", ctx.time(), ctx.id, self.workers.len());
                let total_cpus: u64 = self.workers.values().map(|w| w.cpus_total as u64).sum();
                let available_cpus: u64 = self.workers.values().map(|w| w.cpus_available as u64).sum();
                let total_memory: u64 = self.workers.values().map(|w| w.memory_total as u64).sum();
                let available_memory: u64 = self.workers.values().map(|w| w.memory_available as u64).sum();
                println!(
                    "{} [{}] --- cpus: {} / {}, memory: {} / {}",
                    ctx.time(),
                    ctx.id,
                    available_cpus,
                    total_cpus,
                    available_memory,
                    total_memory,
                );
                let task_count = self.tasks.len();
                let completed_count = self
                    .tasks
                    .values()
                    .filter(|t| matches!(t.state, TaskState::Completed))
                    .count();
                println!(
                    "{} [{}] --- tasks: {} / {}",
                    ctx.time(),
                    ctx.id,
                    completed_count,
                    task_count
                );
                if task_count == 0 || completed_count != task_count {
                    ctx.emit_self(ReportStatus {}, 10.);
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
