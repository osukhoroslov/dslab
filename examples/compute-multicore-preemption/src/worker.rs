use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::async_mode::EventKey;
use dslab_core::{cast, log_debug};
use dslab_core::{Event, EventHandler, Simulation, SimulationContext};

#[derive(Clone, Serialize)]
pub struct Start {}

#[derive(Clone, Serialize, Debug)]
pub struct TaskRequest {
    pub flops: f64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
}

#[derive(Clone, Serialize, Debug)]
pub struct PreemptTask {
    pub id: u64,
}

#[derive(Clone, Serialize, Debug)]
pub struct ContinueTask {
    pub id: u64,
}

pub struct Worker {
    pub compute: Rc<RefCell<Compute>>,
    pub ctx: SimulationContext,
}

impl Worker {
    pub fn new(compute: Rc<RefCell<Compute>>, ctx: SimulationContext) -> Self {
        Self { compute, ctx }
    }

    fn on_start(&self) {
        log_debug!(self.ctx, "started");
    }

    pub fn register_key_getters(sim: &Simulation) {
        sim.register_key_getter_for::<CompStarted>(|e| e.id as EventKey);
        sim.register_key_getter_for::<CompFinished>(|e| e.id as EventKey);
    }

    fn on_task_request(&self, req: TaskRequest) {
        log_debug!(self.ctx, "task request: {:?}", req);
        self.ctx.spawn(self.run(req));
    }

    pub async fn run(&self, req: TaskRequest) {
        let comp_id = self.compute.borrow_mut().run(
            req.flops,
            req.memory,
            req.min_cores,
            req.max_cores,
            req.cores_dependency,
            self.ctx.id(),
        ) as EventKey;
        self.ctx.recv_event_by_key::<CompStarted>(comp_id).await;
        log_debug!(self.ctx, "started execution of task");

        self.ctx.emit_self(PreemptTask { id: comp_id }, 100.);

        self.ctx.emit_self(ContinueTask { id: comp_id }, 200.);

        self.ctx.emit_self(PreemptTask { id: comp_id }, 250.);

        self.ctx.emit_self(ContinueTask { id: comp_id }, 350.);

        self.ctx.recv_event_by_key::<CompFinished>(comp_id).await;
        log_debug!(self.ctx, "completed execution of task");
    }

    pub fn on_preempt_task(&self, id: u64) {
        self.compute.borrow_mut().preempt_computation(id);
    }

    pub fn on_continue_task(&self, id: u64) {
        self.compute.borrow_mut().continue_computation(id);
    }
}

impl EventHandler for Worker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            TaskRequest {
                flops,
                memory,
                min_cores,
                max_cores,
                cores_dependency,
            } => {
                self.on_task_request(TaskRequest {
                    flops,
                    memory,
                    min_cores,
                    max_cores,
                    cores_dependency,
                });
            }
            PreemptTask { id } => {
                self.on_preempt_task(id);
            }
            CompPreempted { fraction_done, .. } => {
                log_debug!(self.ctx, "Task is preempted. Task is {}% done", fraction_done * 100.);
            }
            ContinueTask { id } => {
                self.on_continue_task(id);
            }
            CompContinued { .. } => {
                log_debug!(self.ctx, "Task is continued");
            }
        })
    }
}
