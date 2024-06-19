use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_compute::multicore::*;
use dslab_core::async_mode::EventKey;
use dslab_core::{cast, log_debug, StaticEventHandler};
use dslab_core::{Event, Simulation, SimulationContext};

#[derive(Clone, Serialize, Debug)]
pub struct TaskRequest {
    pub flops: f64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub cores_dependency: CoresDependency,
}

pub struct Worker {
    pub compute: Rc<RefCell<Compute>>,
    pub ctx: SimulationContext,
}

impl Worker {
    pub fn new(compute: Rc<RefCell<Compute>>, ctx: SimulationContext) -> Self {
        Self { compute, ctx }
    }

    pub fn register_key_getters(sim: &Simulation) {
        sim.register_key_getter_for::<CompStarted>(|e| e.id as EventKey);
        sim.register_key_getter_for::<CompFinished>(|e| e.id as EventKey);
        sim.register_key_getter_for::<CompCancelled>(|e| e.id as EventKey);
    }

    fn on_task_request(self: Rc<Self>, req: TaskRequest) {
        log_debug!(self.ctx, "task request: {:?}", req);
        self.ctx.spawn(self.clone().run(req, self.ctx.rand() < 0.5));
    }

    pub async fn run(self: Rc<Self>, req: TaskRequest, preempt: bool) {
        let comp_id = self.compute.borrow_mut().run(
            req.flops,
            req.memory,
            req.min_cores,
            req.max_cores,
            req.cores_dependency,
            self.ctx.id(),
        ) as EventKey;
        self.ctx.recv_event_by_key::<CompStarted>(comp_id).await;
        log_debug!(self.ctx, "Task {} is running", comp_id);

        let min_compute_time = self
            .compute
            .borrow()
            .min_compute_time(req.flops, req.min_cores, req.max_cores, req.cores_dependency)
            .unwrap();

        if preempt {
            self.ctx.sleep(min_compute_time / 4.).await;
            self.compute.borrow_mut().preempt_computation(comp_id);

            self.ctx.sleep(min_compute_time / 4.).await;
            self.compute.borrow_mut().resume_computation(comp_id);

            self.ctx.sleep(min_compute_time / 2.).await;
            self.compute.borrow_mut().preempt_computation(comp_id);

            self.ctx.sleep(min_compute_time / 8.).await;
            self.compute.borrow_mut().resume_computation(comp_id);

            self.ctx.recv_event_by_key::<CompFinished>(comp_id).await;
            log_debug!(self.ctx, "Task {} is completed", comp_id);
        } else {
            // cancel
            self.ctx.sleep(min_compute_time / 2.).await;
            self.compute.borrow_mut().cancel_computation(comp_id);

            self.ctx.recv_event_by_key::<CompCancelled>(comp_id).await;
            log_debug!(self.ctx, "Task {} is cancelled", comp_id);
        }
    }
}

impl StaticEventHandler for Worker {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
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
            CompPreempted { id, fraction_done } => {
                log_debug!(
                    self.ctx,
                    "Task {} is preempted. (fraction_done: {}%)",
                    id,
                    fraction_done * 100.
                );
            }
            CompResumed { id } => {
                log_debug!(self.ctx, "Task {} is resumed", id);
            }
            CompFailed { id, reason } => {
                log_debug!(self.ctx, "Task {} is failed: {:?}", id, reason);
            }
        })
    }
}
