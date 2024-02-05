use log::log_enabled;
use log::Level::Info;
use priority_queue::PriorityQueue;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::{cast, log_debug, log_info, log_trace};
use dslab_network::Network;

use crate::client::{ClientRegister, JobCompleted, JobsInquiry};
use crate::common::Start;
use crate::job::*;

#[derive(Clone, Serialize)]
pub struct ServerRegister {}

#[derive(Clone, Serialize)]
pub struct ReportStatus {}

#[derive(Clone, Serialize)]
pub struct ScheduleJobs {}

#[derive(Clone, Serialize)]
pub struct ValidateResults {}

#[derive(Clone, Serialize)]
pub struct PurgeDB {}

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
    job_generator_id: Id,
    clients: BTreeMap<Id, ClientInfo>,
    client_queue: PriorityQueue<Id, ClientScore>,
    // db
    workunit: HashMap<u64, Rc<RefCell<WorkunitInfo>>>,
    result: HashMap<u64, Rc<RefCell<ResultInfo>>>,
    //
    cpus_total: u32,
    cpus_available: u32,
    memory_total: u64,
    memory_available: u64,
    pub scheduling_time: f64,
    scheduling_planned: bool,
    ctx: SimulationContext,
}

impl Server {
    pub fn new(net: Rc<RefCell<Network>>, job_generator_id: Id, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            net,
            job_generator_id,
            clients: BTreeMap::new(),
            client_queue: PriorityQueue::new(),
            workunit: HashMap::new(),
            result: HashMap::new(),
            cpus_total: 0,
            cpus_available: 0,
            memory_total: 0,
            memory_available: 0,
            scheduling_time: 0.,
            scheduling_planned: false,
            ctx,
        }
    }

    fn on_started(&mut self) {
        log_debug!(self.ctx, "started");
        self.scheduling_planned = true;
        self.ctx.emit_self(ScheduleJobs {}, 1.);
        self.ctx.emit_self(ValidateResults {}, 50.);
        self.ctx.emit_self(PurgeDB {}, 60.);
        if log_enabled!(Info) {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
        self.ctx.emit(ServerRegister {}, self.job_generator_id, 0.5);
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
        self.client_queue.push(client_id, client.score());
        self.clients.insert(client.id, client);
    }

    fn on_job_request(&mut self, req: JobRequest) {
        let workunit = WorkunitInfo {
            id: req.id,
            req,
            need_validate: false,
            canonical_resultid: None,
        };
        let result = ResultInfo {
            id: self.result.len() as u64,
            workunit_id: workunit.id,
            server_state: ResultState::Unsent,
            outcome: None,
            validate_state: None,
        };
        log_debug!(self.ctx, "job request: {:?}", workunit.req);
        self.result.insert(result.id, Rc::new(RefCell::new(result)));
        self.workunit.insert(workunit.req.id, Rc::new(RefCell::new(workunit)));

        if !self.scheduling_planned {
            self.scheduling_planned = true;
            self.ctx.emit_self(ScheduleJobs {}, 10.);
        }
    }

    fn on_result_completed(&mut self, result_id: u64, client_id: Id) {
        log_debug!(self.ctx, "completed result: {:?}", result_id);
        let mut result = self.result.get_mut(&result_id).unwrap().borrow_mut();
        let mut workunit = self.workunit.get_mut(&result.workunit_id).unwrap().borrow_mut();
        result.server_state = ResultState::Over;
        result.outcome = Some(ResultOutcome::Success);
        result.validate_state = Some(ValidateState::Init);
        workunit.need_validate = true;

        let client = self.clients.get_mut(&client_id).unwrap();
        client.cpus_available += workunit.req.min_cores;
        client.memory_available += workunit.req.memory;
        self.cpus_available += workunit.req.min_cores;
        self.memory_available += workunit.req.memory;
    }

    fn on_jobs_inquiry(&mut self, client_id: Id) {
        let client = self.clients.get(&client_id).unwrap();
        self.client_queue.push(client_id, client.score());
    }

    fn schedule_results(&mut self) {
        let unsent_results = self.get_map_keys_by_predicate(&self.result, |result| {
            result.borrow().server_state == ResultState::Unsent
        });
        if !unsent_results.is_empty() {
            log_trace!(self.ctx, "scheduling results");
            let t = Instant::now();
            let mut assigned_results = HashSet::new();
            for result_id in unsent_results {
                if self.client_queue.is_empty() {
                    break;
                }
                let mut result = self.result.get_mut(&result_id).unwrap().borrow_mut();
                let workunit = self.workunit.get(&result.workunit_id).unwrap().borrow();
                if workunit.req.min_cores > self.cpus_available || workunit.req.memory > self.memory_available {
                    continue;
                }
                let mut checked_clients = Vec::new();
                while let Some((client_id, (memory, cpus, speed))) = self.client_queue.pop() {
                    if cpus >= workunit.req.min_cores && memory >= workunit.req.memory {
                        log_debug!(self.ctx, "assigned result {} to client {}", result_id, client_id);
                        result.server_state = ResultState::InProgress;
                        assigned_results.insert(result_id);
                        let client = self.clients.get_mut(&client_id).unwrap();
                        client.cpus_available -= workunit.req.min_cores;
                        client.memory_available -= workunit.req.memory;
                        self.cpus_available -= workunit.req.min_cores;
                        self.memory_available -= workunit.req.memory;
                        checked_clients.push((client.id, client.score()));
                        self.net
                            .borrow_mut()
                            .send_event(workunit.req.clone(), self.id, client_id);
                        break;
                    } else {
                        checked_clients.push((client_id, (memory, cpus, speed)));
                    }
                    if memory <= workunit.req.memory {
                        break;
                    }
                }
                for (client_id, (memory, cpus, speed)) in checked_clients.into_iter() {
                    if memory > 0 && cpus > 0 {
                        self.client_queue.push(client_id, (memory, cpus, speed));
                    }
                }
            }
            let schedule_duration = t.elapsed();
            log_info!(
                self.ctx,
                "schedule_results: assigned {} results in {:.2?}",
                assigned_results.len(),
                schedule_duration
            );
            self.scheduling_time += schedule_duration.as_secs_f64();
        }
        if self.is_active() && !self.scheduling_planned {
            self.scheduling_planned = true;
            self.ctx.emit_self(ScheduleJobs {}, 10.);
        }
        self.scheduling_planned = false;
    }

    fn report_status(&mut self) {
        log_info!(
            self.ctx,
            "CPU: {:.2} / MEMORY: {:.2} / UNASSIGNED: {} / ASSIGNED: {} / COMPLETED: {}",
            (self.cpus_total - self.cpus_available) as f64 / self.cpus_total as f64,
            (self.memory_total - self.memory_available) as f64 / self.memory_total as f64,
            self.get_map_keys_by_predicate(&self.result, |result| {
                result.borrow().server_state == ResultState::Unsent
            })
            .len(),
            self.get_map_keys_by_predicate(&self.result, |result| {
                result.borrow().server_state == ResultState::InProgress
            })
            .len(),
            self.get_map_keys_by_predicate(&self.result, |result| {
                result.borrow().server_state == ResultState::Over
            })
            .len()
        );
        if self.is_active() {
            self.ctx.emit_self(ReportStatus {}, 100.);
        }
    }

    fn validate_results(&mut self) {
        log_info!(self.ctx, "starting validation");
        let need_validation = self.get_map_keys_by_predicate(&self.workunit, |wu| wu.borrow().need_validate == true);
        let mut canonical_result = None;
        let mut validated_count = 0;
        for wu_id in need_validation {
            let result_ids =
                self.get_map_keys_by_predicate(&self.result, |result| result.borrow().workunit_id == wu_id);
            for result_id in result_ids {
                let mut result = self.result.get_mut(&result_id).unwrap().borrow_mut();
                result.validate_state = Some(ValidateState::Valid);
                canonical_result = Some(result_id);
                validated_count += 1;
            }
            let mut workunit = self.workunit.get_mut(&wu_id).unwrap().borrow_mut();
            workunit.need_validate = false;
            workunit.canonical_resultid = canonical_result;
        }
        log_info!(self.ctx, "validated {} results", validated_count);
        if self.is_active() {
            self.ctx.emit_self(ValidateResults {}, 50.);
        }
    }

    fn purge_db(&mut self) {}

    fn get_map_keys_by_predicate<K: Clone, V, F>(&self, hm: &HashMap<K, V>, predicate: F) -> Vec<K>
    where
        F: Fn(&V) -> bool,
    {
        hm.iter()
            .filter(|(_, v)| predicate(*v))
            .map(|(k, _)| (*k).clone())
            .collect::<Vec<_>>()
    }

    fn is_active(&self) -> bool {
        !self
            .get_map_keys_by_predicate(&self.result, |result| {
                result.borrow().server_state == ResultState::Unsent
            })
            .is_empty()
            || !self
                .get_map_keys_by_predicate(&self.result, |result| {
                    result.borrow().server_state == ResultState::InProgress
                })
                .is_empty()
    }
}

impl EventHandler for Server {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_started();
            }
            ScheduleJobs {} => {
                self.schedule_results();
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
            JobCompleted { id } => {
                self.on_result_completed(id, event.src);
            }
            ReportStatus {} => {
                self.report_status();
            }
            JobsInquiry {} => {
                self.on_jobs_inquiry(event.src)
            }
            ValidateResults {} => {
                self.validate_results();
            }
            PurgeDB {} => {
                self.purge_db();
            }
        })
    }
}
