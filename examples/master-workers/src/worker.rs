use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::Start;
use crate::storage::*;
use crate::task::*;
use compute::multicore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use network::model::DataTransferCompleted;
use network::network::Network;

#[derive(Debug)]
pub struct WorkerRegister {
    pub(crate) speed: u64,
    pub(crate) cpus_total: u32,
    pub(crate) memory_total: u64,
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
    tasks: HashMap<u64, TaskInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
}

impl Worker {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        storage: Rc<RefCell<Storage>>,
        net: Rc<RefCell<Network>>,
        master: ActorId,
    ) -> Self {
        Self {
            compute,
            storage,
            net,
            master,
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
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
                ctx.emit_now(
                    WorkerRegister {
                        speed: self.compute.borrow().speed(),
                        cpus_total: self.compute.borrow().cores_total(),
                        memory_total: self.compute.borrow().memory_total(),
                    },
                    self.master.clone(),
                );
            }
            TaskRequest {
                id,
                flops: _,
                memory: _,
                min_cores: _,
                max_cores: _,
                cores_dependency: _,
                input_size,
                output_size: _,
            } => {
                println!("{} [{}] task request: {:?}", ctx.time(), ctx.id, event);
                let task = TaskInfo {
                    req: *event.downcast_ref::<TaskRequest>().unwrap(),
                    state: TaskState::Downloading,
                };
                self.tasks.insert(*id, task);

                let transfer_id = self.net.borrow().transfer_data(
                    self.master.clone(),
                    ctx.id.clone(),
                    *input_size as f64,
                    ctx.id.clone(),
                    ctx,
                );
                self.downloads.insert(transfer_id, *id);
            }
            DataTransferCompleted { data } => {
                // data transfer corresponds to input download
                let transfer_id = data.id;
                if self.downloads.contains_key(&transfer_id) {
                    let task_id = self.downloads.remove(&transfer_id).unwrap();
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
                } else if self.uploads.contains_key(&transfer_id) {
                    let task_id = self.uploads.remove(&transfer_id).unwrap();
                    let mut task = self.tasks.remove(&task_id).unwrap();
                    println!("{} [{}] uploaded output data for task: {}", ctx.time(), ctx.id, task_id);
                    task.state = TaskState::Completed;
                    self.net
                        .borrow()
                        .send_event(TaskCompleted { id: task_id }, self.master.clone(), ctx);
                }
            }
            DataReadCompleted { id } => {
                let task_id = self.reads.remove(id).unwrap();
                println!("{} [{}] read input data for task: {}", ctx.time(), ctx.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Running;
                let comp_id = self.compute.borrow().run(
                    task.req.flops,
                    task.req.memory,
                    task.req.min_cores,
                    task.req.max_cores,
                    task.req.cores_dependency,
                    ctx,
                );
                self.computations.insert(comp_id, task_id);
            }
            CompStarted { id, cores: _ } => {
                println!("{} [{}] started execution of task: {}", ctx.time(), ctx.id, id);
            }
            CompFinished { id } => {
                let task_id = self.computations.remove(id).unwrap();
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
                let transfer_id = self.net.borrow().transfer_data(
                    ctx.id.clone(),
                    self.master.clone(),
                    task.req.output_size as f64,
                    ctx.id.clone(),
                    ctx,
                );
                self.uploads.insert(transfer_id, task_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
