use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::Start;
use crate::compute::*;
use crate::network::*;
use crate::task::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;

#[derive(Debug)]
pub struct WorkerRegister {
    pub(crate) speed: u64,
    pub(crate) total_cpus: u32,
}

#[derive(Debug)]
pub struct TaskCompleted {
    pub(crate) id: u64,
}

pub struct Worker {
    compute: Rc<RefCell<Compute>>,
    net: Rc<RefCell<Network>>,
    master: ActorId,
    total_cpus: u32,
    used_cpus: u32,
    tasks: HashMap<u64, TaskInfo>,
}

impl Worker {
    pub fn new(compute: Rc<RefCell<Compute>>, net: Rc<RefCell<Network>>, master: ActorId) -> Self {
        let total_cpus = compute.borrow().cpus();
        Self {
            compute,
            net,
            master,
            total_cpus,
            used_cpus: 0,
            tasks: HashMap::new(),
        }
    }
}

impl Actor for Worker {
    #[allow(unused_variables)]
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
                ctx.emit_now(
                    WorkerRegister {
                        speed: self.compute.borrow().speed(),
                        total_cpus: self.total_cpus,
                    },
                    self.master.clone(),
                );
            }
            TaskRequest {
                id,
                comp_size,
                input_size,
                output_size,
            } => {
                println!("{} [{}] task request: {:?}", ctx.time(), ctx.id, event);
                let task = TaskInfo {
                    req: *event.downcast_ref::<TaskRequest>().unwrap(),
                    state: TaskState::Accepted,
                };
                self.tasks.insert(*id, task);
                self.net
                    .borrow()
                    .transfer(*id, self.master.clone(), ctx.id.clone(), *input_size, ctx);
            }
            DataTransferCompleted { id } => {
                let task = self.tasks.get_mut(id).unwrap();
                match task.state {
                    // data transfer corresponds to input download
                    TaskState::Accepted => {
                        println!("{} [{}] downloaded input data for task: {}", ctx.time(), ctx.id, id);
                        task.state = TaskState::StagedIn;
                        self.compute.borrow().run(*id, task.req.comp_size, ctx);
                        self.used_cpus += 1;
                    }
                    // data transfer corresponds to output upload
                    TaskState::Finished => {
                        println!("{} [{}] uploaded output data for task: {}", ctx.time(), ctx.id, id);
                        task.state = TaskState::StagedOut;
                        self.tasks.remove(id);
                        ctx.emit(TaskCompleted { id: *id }, self.master.clone(), 0.5);
                    }
                    _ => {}
                }
            }
            CompFinished { id } => {
                println!("{} [{}] completed execution of task: {}", ctx.time(), ctx.id, id);
                let task = self.tasks.get_mut(id).unwrap();
                task.state = TaskState::Finished;
                self.used_cpus -= 1;
                self.net
                    .borrow()
                    .transfer(*id, ctx.id.clone(), self.master.clone(), task.req.output_size, ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
