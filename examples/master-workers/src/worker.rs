use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug};
use dslab_network::model::*;
use dslab_network::network::Network;
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataWriteCompleted};
use dslab_storage::storage::Storage;

use crate::common::Start;
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
    id: Id,
    compute: Rc<RefCell<Compute>>,
    disk: Disk,
    net: Rc<RefCell<Network>>,
    master_id: Id,
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
        disk: Disk,
        net: Rc<RefCell<Network>>,
        master_id: Id,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            compute,
            disk,
            net,
            master_id,
            tasks: HashMap::new(),
            computations: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            downloads: HashMap::new(),
            uploads: HashMap::new(),
            ctx,
        }
    }

    fn on_start(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit(
            WorkerRegister {
                speed: self.compute.borrow().speed(),
                cpus_total: self.compute.borrow().cores_total(),
                memory_total: self.compute.borrow().memory_total(),
            },
            self.master_id,
            0.5,
        );
    }

    fn on_task_request(&mut self, req: TaskRequest) {
        let task = TaskInfo {
            req,
            state: TaskState::Downloading,
        };
        log_debug!(self.ctx, "task request: {:?}", task.req);
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(self.master_id, self.id, task.req.input_size as f64, self.id);
        self.downloads.insert(transfer_id, task.req.id);
        self.tasks.insert(task.req.id, task);
    }

    fn on_data_transfer_completed(&mut self, data: Data) {
        // data transfer corresponds to input download
        let transfer_id = data.id;
        if self.downloads.contains_key(&transfer_id) {
            let task_id = self.downloads.remove(&transfer_id).unwrap();
            let task = self.tasks.get_mut(&task_id).unwrap();
            log_debug!(self.ctx, "downloaded input data for task: {}", task_id);
            task.state = TaskState::Reading;
            let read_id = self.disk.read(task.req.input_size, self.id);
            self.reads.insert(read_id, task_id);
        // data transfer corresponds to output upload
        } else if self.uploads.contains_key(&transfer_id) {
            let task_id = self.uploads.remove(&transfer_id).unwrap();
            let mut task = self.tasks.remove(&task_id).unwrap();
            log_debug!(self.ctx, "uploaded output data for task: {}", task_id);
            task.state = TaskState::Completed;
            self.disk
                .mark_free(task.req.output_size)
                .expect("Failed to free disk space");
            self.net
                .borrow_mut()
                .send_event(TaskCompleted { id: task_id }, self.id, self.master_id);
        }
    }

    fn on_data_read_completed(&mut self, request_id: u64) {
        let task_id = self.reads.remove(&request_id).unwrap();
        log_debug!(self.ctx, "read input data for task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Running;
        let comp_id = self.compute.borrow_mut().run(
            task.req.flops,
            task.req.memory,
            task.req.min_cores,
            task.req.max_cores,
            task.req.cores_dependency,
            self.id,
        );
        self.computations.insert(comp_id, task_id);
    }

    fn on_comp_started(&mut self, comp_id: u64) {
        let task_id = self.computations.get(&comp_id).unwrap();
        log_debug!(self.ctx, "started execution of task: {}", task_id);
    }

    fn on_comp_finished(&mut self, comp_id: u64) {
        let task_id = self.computations.remove(&comp_id).unwrap();
        log_debug!(self.ctx, "completed execution of task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Writing;
        let write_id = self.disk.write(task.req.output_size, self.id);
        self.writes.insert(write_id, task_id);
    }

    fn on_data_write_completed(&mut self, request_id: u64) {
        let task_id = self.writes.remove(&request_id).unwrap();
        log_debug!(self.ctx, "wrote output data for task: {}", task_id);
        let task = self.tasks.get_mut(&task_id).unwrap();
        task.state = TaskState::Uploading;
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(self.id, self.master_id, task.req.output_size as f64, self.id);
        self.uploads.insert(transfer_id, task_id);
    }
}

impl EventHandler for Worker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
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
            DataTransferCompleted { data } => {
                self.on_data_transfer_completed(data);
            }
            DataReadCompleted { request_id, size: _ } => {
                self.on_data_read_completed(request_id);
            }
            CompStarted { id, cores: _ } => {
                self.on_comp_started(id);
            }
            CompFinished { id } => {
                self.on_comp_finished(id);
            }
            DataWriteCompleted { request_id, size: _ } => {
                self.on_data_write_completed(request_id);
            }
        })
    }
}
