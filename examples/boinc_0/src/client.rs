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
use dslab_network::{DataTransfer, DataTransferCompleted, Network};
use dslab_storage::disk::Disk;
use dslab_storage::events::{DataReadCompleted, DataWriteCompleted};
use dslab_storage::storage::Storage;

use crate::common::Start;
use crate::job::*;
use crate::job_generator::NoMoreJobs;

#[derive(Clone, Serialize)]
pub struct JobsInquiry {}

#[derive(Clone, Serialize)]
pub struct AskForJobs {}

#[derive(Clone, Serialize)]
pub struct ClientRegister {
    pub(crate) speed: f64,
    pub(crate) cpus_total: u32,
    pub(crate) memory_total: u64,
}

#[derive(Clone, Serialize)]
pub struct JobCompleted {
    pub(crate) id: u64,
}

pub struct Client {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    disk: Rc<RefCell<Disk>>,
    net: Rc<RefCell<Network>>,
    server_id: Id,
    jobs: HashMap<u64, JobInfo>,
    computations: HashMap<u64, u64>,
    reads: HashMap<u64, u64>,
    writes: HashMap<u64, u64>,
    downloads: HashMap<usize, u64>,
    uploads: HashMap<usize, u64>,
    all_jobs_received: bool,
    ctx: SimulationContext,
}

impl Client {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        disk: Rc<RefCell<Disk>>,
        net: Rc<RefCell<Network>>,
        server_id: Id,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            compute,
            disk,
            net,
            server_id,
            jobs: HashMap::new(),
            computations: HashMap::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            downloads: HashMap::new(),
            uploads: HashMap::new(),
            all_jobs_received: false,
            ctx,
        }
    }

    fn on_start(&mut self) {
        log_debug!(self.ctx, "started");
        self.ctx.emit(
            ClientRegister {
                speed: self.compute.borrow().speed(),
                cpus_total: self.compute.borrow().cores_total(),
                memory_total: self.compute.borrow().memory_total(),
            },
            self.server_id,
            0.5,
        );
        self.ctx.emit_self(AskForJobs {}, 100.);
    }

    fn on_job_request(&mut self, req: JobRequest) {
        let job = JobInfo {
            req,
            state: JobState::Downloading,
        };
        log_debug!(self.ctx, "job request: {:?}", job.req);
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(self.server_id, self.id, job.req.input_size as f64, self.id);
        self.downloads.insert(transfer_id, job.req.id);
        self.jobs.insert(job.req.id, job);
    }

    fn on_data_transfer_completed(&mut self, dt: DataTransfer) {
        // data transfer corresponds to input download
        let transfer_id = dt.id;
        if self.downloads.contains_key(&transfer_id) {
            let job_id = self.downloads.remove(&transfer_id).unwrap();
            let job = self.jobs.get_mut(&job_id).unwrap();
            log_debug!(self.ctx, "downloaded input data for job: {}", job_id);
            job.state = JobState::Reading;
            let read_id = self.disk.borrow_mut().read(job.req.input_size, self.id);
            self.reads.insert(read_id, job_id);
        // data transfer corresponds to output upload
        } else if self.uploads.contains_key(&transfer_id) {
            let job_id = self.uploads.remove(&transfer_id).unwrap();
            let mut job = self.jobs.remove(&job_id).unwrap();
            log_debug!(self.ctx, "uploaded output data for job: {}", job_id);
            job.state = JobState::Completed;
            self.disk
                .borrow_mut()
                .mark_free(job.req.output_size)
                .expect("Failed to free disk space");
            self.net
                .borrow_mut()
                .send_event(JobCompleted { id: job_id }, self.id, self.server_id);
            self.ask_for_jobs();
        }
    }

    fn on_data_read_completed(&mut self, request_id: u64) {
        let job_id = self.reads.remove(&request_id).unwrap();
        log_debug!(self.ctx, "read input data for job: {}", job_id);
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.state = JobState::Running;
        let comp_id = self.compute.borrow_mut().run(
            job.req.flops,
            job.req.memory,
            job.req.min_cores,
            job.req.max_cores,
            job.req.cores_dependency,
            self.id,
        );
        self.computations.insert(comp_id, job_id);
    }

    fn on_comp_started(&mut self, comp_id: u64) {
        let job_id = self.computations.get(&comp_id).unwrap();
        log_debug!(self.ctx, "started execution of job: {}", job_id);
    }

    fn on_comp_finished(&mut self, comp_id: u64) {
        let job_id = self.computations.remove(&comp_id).unwrap();
        log_debug!(self.ctx, "completed execution of job: {}", job_id);
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.state = JobState::Writing;
        let write_id = self.disk.borrow_mut().write(job.req.output_size, self.id);
        self.writes.insert(write_id, job_id);
    }

    // Uploading results of completed jobs to server
    fn on_data_write_completed(&mut self, request_id: u64) {
        let job_id = self.writes.remove(&request_id).unwrap();
        log_debug!(self.ctx, "wrote output data for job: {}", job_id);
        let job = self.jobs.get_mut(&job_id).unwrap();
        job.state = JobState::Uploading;
        let transfer_id =
            self.net
                .borrow_mut()
                .transfer_data(self.id, self.server_id, job.req.output_size as f64, self.id);
        self.uploads.insert(transfer_id, job_id);
    }

    fn ask_for_jobs(&mut self) {
        if !self.all_jobs_received {
            self.net
                .borrow_mut()
                .send_event(JobsInquiry {}, self.id, self.server_id);
            self.ctx.emit_self(AskForJobs {}, 100.);
        }
    }

    fn on_no_more_jobs(&mut self) {
        self.all_jobs_received = true;
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            JobRequest {
                id,
                flops,
                memory,
                min_cores,
                max_cores,
                cores_dependency,
                input_size,
                output_size,
            } => {
                self.on_job_request(JobRequest {
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
            NoMoreJobs {} => {
                self.on_no_more_jobs();
            }
            DataTransferCompleted { dt } => {
                self.on_data_transfer_completed(dt);
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
            AskForJobs {} => {
                self.ask_for_jobs();
            }
        })
    }
}
