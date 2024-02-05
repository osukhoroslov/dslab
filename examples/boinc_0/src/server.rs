use log::log_enabled;
use log::Level::Info;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, log_info, log_trace};
use dslab_network::Network;

use crate::client::{ClientRegister, JobCompleted, JobsInquiry};
use crate::common::Start;
use crate::job::*;
use crate::job_generator::NoMoreJobs;

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct ScheduleJobs {}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum ClientState {
    Online,
    Offline,
}

#[derive(Debug)]
pub struct ClientInfo {
    id: Id,
    #[allow(dead_code)]
    state: ClientState,
    speed: f64,
    #[allow(dead_code)]
    cpus_total: u32,
    cpus_available: u32,
    #[allow(dead_code)]
    memory_total: u64,
    memory_available: u64,
}

type ClientScore = (u64, u32, u64);

impl ClientInfo {
    pub fn score(&self) -> ClientScore {
        (self.memory_available, self.cpus_available, (self.speed * 1000.) as u64)
    }
}

pub struct Server {
    id: Id,
    net: Rc<RefCell<Network>>,
    clients: BTreeMap<Id, ClientInfo>,
    unassigned_jobs: BTreeMap<u64, JobInfo>,
    assigned_jobs: HashMap<u64, JobInfo>,
    completed_jobs: HashMap<u64, JobInfo>,
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
    all_jobs_received: bool,
    pub scheduling_time: f64,
    ctx: SimulationContext,
}

impl Server {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            clients: BTreeMap::new(),
            unassigned_jobs: BTreeMap::new(),
            assigned_jobs: HashMap::new(),
            completed_jobs: HashMap::new(),
            cpus_total: 0,
            cpus_available: 0,
            memory_total: 0,
            memory_available: 0,
            all_jobs_received: false,
            scheduling_time: 0.,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        if log_enabled!(Info) {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn on_client_register(&mut self, client_id: Id, cpus_total: u32, memory_total: u64, speed: f64) {
        let client = ClientInfo {
            id: client_id,
            state: ClientState::Online,
            speed,
            cpus_total,
            cpus_available: cpus_total,
            memory_total,
            memory_available: memory_total,
        };
        log_debug!(self.ctx, "registered client: {:?}", client);
        self.cpus_total += client.cpus_total;
        self.cpus_available += client.cpus_available;
        self.memory_total += client.memory_total;
        self.memory_available += client.memory_available;
        self.clients.insert(client.id, client);
    }

    fn on_job_request(&mut self, req: JobRequest) {
        let job = JobInfo {
            req,
            state: JobState::New,
        };
        log_debug!(self.ctx, "job request: {:?}", job.req);
        self.unassigned_jobs.insert(job.req.id, job);
    }

    fn on_job_completed(&mut self, job_id: u64, client_id: Id) {
        log_debug!(self.ctx, "completed job: {:?}", job_id);
        let mut job = self.assigned_jobs.remove(&job_id).unwrap();
        job.state = JobState::Completed;
        let client = self.clients.get_mut(&client_id).unwrap();
        client.cpus_available += job.req.min_cores;
        client.memory_available += job.req.memory;
        self.cpus_available += job.req.min_cores;
        self.memory_available += job.req.memory;
        self.completed_jobs.insert(job_id, job);
    }

    fn on_jobs_inquiry(&mut self, client_id: Id) {
        if self.unassigned_jobs.is_empty() && self.all_jobs_received {
            self.net.borrow_mut().send_event(NoMoreJobs {}, self.id, client_id);
        }
        if self.unassigned_jobs.is_empty() {
            return;
        }
        log_trace!(self.ctx, "scheduling jobs for client {}", client_id);
        let mut assigned_jobs = HashSet::new();
        let client = self.clients.get_mut(&client_id).unwrap();
        for (job_id, job) in self.unassigned_jobs.iter_mut() {
            if job.req.min_cores > self.cpus_available || job.req.memory > self.memory_available {
                continue;
            }
            let (memory, cpus, _) = client.score();
            if cpus >= job.req.min_cores && memory >= job.req.memory {
                log_debug!(self.ctx, "assigned job {} to client {}", job_id, client_id);
                job.state = JobState::Assigned;
                assigned_jobs.insert(*job_id);
                client.cpus_available -= job.req.min_cores;
                client.memory_available -= job.req.memory;
                self.cpus_available -= job.req.min_cores;
                self.memory_available -= job.req.memory;
                self.net.borrow_mut().send_event(job.req.clone(), self.id, client_id);
            }
            if memory <= job.req.memory {
                break;
            }
        }
        for job_id in assigned_jobs.into_iter() {
            let job = self.unassigned_jobs.remove(&job_id).unwrap();
            self.assigned_jobs.insert(job_id, job);
        }
    }

    fn report_status(&mut self) {
        log_info!(
            self.ctx,
            "CPU: {:.2} / MEMORY: {:.2} / UNASSIGNED: {} / ASSIGNED: {} / COMPLETED: {}",
            (self.cpus_total - self.cpus_available) as f64 / self.cpus_total as f64,
            (self.memory_total - self.memory_available) as f64 / self.memory_total as f64,
            self.unassigned_jobs.len(),
            self.assigned_jobs.len(),
            self.completed_jobs.len()
        );
        if self.is_active() {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn on_no_more_jobs(&mut self) {
        self.all_jobs_received = true;
    }

    fn is_active(&self) -> bool {
        !self.unassigned_jobs.is_empty() || !self.assigned_jobs.is_empty()
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_started();
            }
            ClientRegister {
                speed,
                cpus_total,
                memory_total,
            } => {
                self.on_client_register(event.src, cpus_total, memory_total, speed);
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
            JobCompleted { id } => {
                self.on_job_completed(id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
            JobsInquiry {} => {
                self.on_jobs_inquiry(event.src)
            }
        })
    }
}
