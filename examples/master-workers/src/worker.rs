use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Serialize;

use compute::multicore::*;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::{cast, log_debug};
use network::model::*;
use network::network::Network;

use crate::common::Start;
use crate::storage::*;
use crate::task::*;

#[derive(Serialize)]
pub struct WorkerRegister {
    pub(crate) speed: u64,
    pub(crate) cpus_total: u32,
    pub(crate) memory_total: u64,
}

#[derive(Serialize)]
pub struct TaskCompleted {
    pub(crate) id: u64,
}

pub struct Worker {
    id: String,
    compute: Rc<RefCell<Compute>>,
    storage: Storage,
    net: Rc<RefCell<Network>>,
    master: String,
    tasks: HashMap<u64, TaskInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
    ctx: SimulationContext,
}

impl Worker {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        storage: Storage,
        net: Rc<RefCell<Network>>,
        master: String,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id().to_string(),
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
            ctx,
        }
    }
}

impl EventHandler for Worker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                log_debug!(self.ctx, "started");
                self.ctx.emit(
                    WorkerRegister {
                        speed: self.compute.borrow().speed(),
                        cpus_total: self.compute.borrow().cores_total(),
                        memory_total: self.compute.borrow().memory_total(),
                    },
                    &self.master,
                    0.5,
                );
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
                    state: TaskState::Downloading,
                };
                log_debug!(self.ctx, "task request: {:?}", task.req);
                self.tasks.insert(id, task);

                let transfer_id =
                    self.net
                        .borrow_mut()
                        .transfer_data(&self.master, &self.id, input_size as f64, &self.id);
                self.downloads.insert(transfer_id, id);
            }
            DataTransferCompleted { data } => {
                // data transfer corresponds to input download
                let transfer_id = data.id;
                if self.downloads.contains_key(&transfer_id) {
                    let task_id = self.downloads.remove(&transfer_id).unwrap();
                    let task = self.tasks.get_mut(&task_id).unwrap();
                    log_debug!(self.ctx, "downloaded input data for task: {}", task_id);
                    task.state = TaskState::Reading;
                    let read_id = self.storage.read(task.req.input_size, &self.id);
                    self.reads.insert(read_id, task_id);
                // data transfer corresponds to output upload
                } else if self.uploads.contains_key(&transfer_id) {
                    let task_id = self.uploads.remove(&transfer_id).unwrap();
                    let mut task = self.tasks.remove(&task_id).unwrap();
                    log_debug!(self.ctx, "uploaded output data for task: {}", task_id);
                    task.state = TaskState::Completed;
                    self.net
                        .borrow_mut()
                        .send_event(TaskCompleted { id: task_id }, &self.id, &self.master);
                }
            }
            DataReadCompleted { id } => {
                let task_id = self.reads.remove(&id).unwrap();
                log_debug!(self.ctx, "read input data for task: {}", task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Running;
                let comp_id = self.compute.borrow_mut().run(
                    task.req.flops,
                    task.req.memory,
                    task.req.min_cores,
                    task.req.max_cores,
                    task.req.cores_dependency,
                    &self.id,
                );
                self.computations.insert(comp_id, task_id);
            }
            CompStarted { id, cores: _ } => {
                log_debug!(self.ctx, "started execution of task: {}", id);
            }
            CompFinished { id } => {
                let task_id = self.computations.remove(&id).unwrap();
                log_debug!(self.ctx, "completed execution of task: {}", task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Writing;
                let write_id = self.storage.write(task.req.output_size, &self.id);
                self.writes.insert(write_id, task_id);
            }
            DataWriteCompleted { id } => {
                let task_id = self.writes.remove(&id).unwrap();
                log_debug!(self.ctx, "wrote output data for task: {}", task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Uploading;
                let transfer_id =
                    self.net
                        .borrow_mut()
                        .transfer_data(&self.id, &self.master, task.req.output_size as f64, &self.id);
                self.uploads.insert(transfer_id, task_id);
            }
        })
    }
}
