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

#[derive(Debug)]
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
    total_cpus: u32,
    used_cpus: u32,
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
                println!("{} [{}] - scheduling task {}", ctx.time(), ctx.id, task_id);
                for (worker_id, worker) in self.workers.iter_mut() {
                    if worker.used_cpus < worker.total_cpus {
                        println!(
                            "{} [{}] - assigned task {} to worker {}",
                            ctx.time(),
                            ctx.id,
                            task_id,
                            worker_id
                        );
                        task.state = TaskState::Assigned;
                        worker.used_cpus += 1;
                        self.net.borrow().send(task.req, worker_id.clone(), ctx);
                        break;
                    }
                }
            }
        }
    }
}

impl Actor for Master {
    #[allow(unused_variables)]
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
            }
            WorkerRegister { speed, total_cpus } => {
                let worker = WorkerInfo {
                    id: from,
                    state: WorkerState::Online,
                    speed: *speed,
                    total_cpus: *total_cpus,
                    used_cpus: 0,
                };
                println!("{} [{}] registered worker: {:?}", ctx.time(), ctx.id, worker);
                self.workers.insert(worker.id.clone(), worker);
            }
            TaskRequest {
                id,
                comp_size,
                input_size,
                output_size,
            } => {
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
                worker.used_cpus -= 1;
                self.schedule_tasks(ctx);
            }
            ReportStatus {} => {
                println!("{} [{}] workers: {}", ctx.time(), ctx.id, self.workers.len());
                let total_cpus: u32 = self.workers.values().map(|w| w.total_cpus).sum();
                let used_cpus: u32 = self.workers.values().map(|w| w.used_cpus).sum();
                println!(
                    "{} [{}] cpus: total - {}, used - {}",
                    ctx.time(),
                    ctx.id,
                    total_cpus,
                    used_cpus
                );
                let task_count = self.tasks.len();
                let completed_count = self
                    .tasks
                    .values()
                    .filter(|t| matches!(t.state, TaskState::Completed))
                    .count();
                println!(
                    "{} [{}] tasks: total - {}, completed - {}",
                    ctx.time(),
                    ctx.id,
                    task_count,
                    completed_count
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
