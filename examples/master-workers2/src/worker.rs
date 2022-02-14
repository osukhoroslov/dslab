use log::debug;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::Start;
use crate::compute::*;
use crate::network::*;
use crate::storage::*;
use crate::task::*;
use core2::cast;
use core2::context::SimulationContext;
use core2::event::Event;
use core2::handler::EventHandler;

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
    id: String,
    compute: Compute,
    storage: Storage,
    net: Rc<RefCell<Network>>,
    master: String,
    tasks: HashMap<u64, TaskInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<u64, u64>,
    uploads: HashMap<u64, u64>,
    ctx: SimulationContext,
}

impl Worker {
    pub fn new(
        compute: Compute,
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
                debug!("{} [{}] started", event.time, self.id);
                self.ctx.emit_now(
                    WorkerRegister {
                        speed: self.compute.speed(),
                        cpus_total: self.compute.cpus_total(),
                        memory_total: self.compute.memory_total(),
                    },
                    &self.master,
                );
            }
            TaskRequest {
                id,
                flops: _,
                cpus: _,
                memory: _,
                input_size,
                output_size: _,
            } => {
                debug!("{} [{}] task request: {:?}", event.time, self.id, event.data);
                let task = TaskInfo {
                    req: *event.data.downcast_ref::<TaskRequest>().unwrap(),
                    state: TaskState::Downloading,
                };
                self.tasks.insert(*id, task);

                let transfer_id = self
                    .net
                    .borrow_mut()
                    .transfer(&self.master, &self.id, *input_size, &self.id);
                self.downloads.insert(transfer_id, *id);
            }
            DataTransferCompleted { id } => {
                // data transfer corresponds to input download
                if self.downloads.contains_key(id) {
                    let task_id = self.downloads.remove(id).unwrap();
                    let task = self.tasks.get_mut(&task_id).unwrap();
                    debug!(
                        "{} [{}] downloaded input data for task: {}",
                        event.time, self.id, task_id
                    );
                    task.state = TaskState::Reading;
                    let read_id = self.storage.read(task.req.input_size, &self.id);
                    self.reads.insert(read_id, task_id);
                // data transfer corresponds to output upload
                } else if self.uploads.contains_key(id) {
                    let task_id = self.uploads.remove(id).unwrap();
                    let task = self.tasks.get_mut(&task_id).unwrap();
                    debug!(
                        "{} [{}] uploaded output data for task: {}",
                        event.time, self.id, task_id
                    );
                    task.state = TaskState::Completed;
                    self.tasks.remove(id);
                    self.net
                        .borrow_mut()
                        .send(TaskCompleted { id: task_id }, &self.id, &self.master);
                }
            }
            DataReadCompleted { id } => {
                let task_id = self.reads.remove(id).unwrap();
                debug!("{} [{}] read input data for task: {}", event.time, self.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Running;
                let comp_id = self.compute.run(task.req.flops, &self.id);
                self.computations.insert(comp_id, task_id);
            }
            CompFinished { id } => {
                let task_id = self.computations.remove(id).unwrap();
                debug!("{} [{}] completed execution of task: {}", event.time, self.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Writing;
                let write_id = self.storage.write(task.req.output_size, &self.id);
                self.writes.insert(write_id, task_id);
            }
            DataWriteCompleted { id } => {
                let task_id = self.writes.remove(id).unwrap();
                debug!("{} [{}] wrote output data for task: {}", event.time, self.id, task_id);
                let task = self.tasks.get_mut(&task_id).unwrap();
                task.state = TaskState::Uploading;
                let transfer_id =
                    self.net
                        .borrow_mut()
                        .transfer(&self.id, &self.master, task.req.output_size, &self.id);
                self.uploads.insert(transfer_id, task_id);
            }
        })
    }
}
