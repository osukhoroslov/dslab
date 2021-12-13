use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::Start;
use crate::compute::*;
use crate::network::*;
use crate::storage::*;
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
    storage: Rc<RefCell<Storage>>,
    net: Rc<RefCell<Network>>,
    master: ActorId,
    total_cpus: u32,
    used_cpus: u32,
    tasks: HashMap<u64, TaskInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<u64, u64>,
    uploads: HashMap<u64, u64>,
}

impl Worker {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        storage: Rc<RefCell<Storage>>,
        net: Rc<RefCell<Network>>,
        master: ActorId,
    ) -> Self {
        let total_cpus = compute.borrow().cpus();
        Self {
            compute,
            storage,
            net,
            master,
            total_cpus,
            used_cpus: 0,
            tasks: HashMap::new(),
            computations: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            downloads: HashMap::new(),
            uploads: HashMap::new(),
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
                    state: TaskState::Downloading,
                };
                self.tasks.insert(*id, task);

                let transfer_id = self
                    .net
                    .borrow()
                    .transfer(self.master.clone(), ctx.id.clone(), *input_size, ctx);
                self.downloads.insert(transfer_id, *id);
            }
            DataTransferCompleted { id } => {
                // data transfer corresponds to input download
                if self.downloads.contains_key(id) {
                    let task_id = self.downloads.remove(id).unwrap();
                    let task = self.tasks.get_mut(&task_id).unwrap();
                    println!(
                        "{} [{}] downloaded input data for task: {}",
                        ctx.time(),
                        ctx.id,
                        task_id
                    );
                    task.state = TaskState::Reading;
                    let read_id = self.storage.borrow().read(task.req.input_size, ctx);
                    self.reads.insert(read_id, task_id);
                // data transfer corresponds to output upload
                } else if self.uploads.contains_key(id) {
                    let task_id = self.uploads.remove(id).unwrap();
                    let task = self.tasks.get_mut(&task_id).unwrap();
                    println!("{} [{}] uploaded output data for task: {}", ctx.time(), ctx.id, task_id);
                    task.state = TaskState::Completed;
                    self.tasks.remove(id);
                    ctx.emit(TaskCompleted { id: task_id }, self.master.clone(), 0.5);
                }
            }
            DataReadCompleted { id } => {
                let task_id = self.reads.remove(id).unwrap();
                println!("{} [{}] read input data for task: {}", ctx.time(), ctx.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Running;
                let comp_id = self.compute.borrow().run(task.req.comp_size, ctx);
                self.computations.insert(comp_id, task_id);
                self.used_cpus += 1;
            }
            CompFinished { id } => {
                let task_id = self.computations.remove(id).unwrap();
                self.used_cpus -= 1;
                println!("{} [{}] completed execution of task: {}", ctx.time(), ctx.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Writing;
                let write_id = self.storage.borrow().write(task.req.output_size, ctx);
                self.writes.insert(write_id, task_id);
            }
            DataWriteCompleted { id } => {
                let task_id = self.writes.remove(id).unwrap();
                println!("{} [{}] wrote output data for task: {}", ctx.time(), ctx.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Uploading;
                let transfer_id =
                    self.net
                        .borrow()
                        .transfer(ctx.id.clone(), self.master.clone(), task.req.output_size, ctx);
                self.uploads.insert(transfer_id, task_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
