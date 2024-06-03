use std::collections::{HashMap, VecDeque};
use std::{cell::RefCell, rc::Rc};

use futures::future::FutureExt;
use futures::select;
use serde_json::json;

use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted, Compute};
use dslab_core::async_mode::EventKey;
use dslab_core::{cast, log_debug, Event, Id, SimulationContext, StaticEventHandler};

use crate::events::{Start, TaskRequest};

pub struct Worker {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    ctx: SimulationContext,
}

impl Worker {
    pub fn new(compute: Rc<RefCell<Compute>>, ctx: SimulationContext) -> Self {
        // register key getters for compute events
        ctx.register_key_getter_for::<CompStarted>(|e| e.id);
        ctx.register_key_getter_for::<CompFailed>(|e| e.id);
        Self {
            id: ctx.id(),
            compute,
            ctx,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    fn on_start(self: Rc<Self>) {
        log_debug!(self.ctx, "Worker started");
        self.ctx.spawn(self.clone().dispatch_loop());
    }

    async fn dispatch_loop(self: Rc<Self>) {
        let mut pending_tasks: VecDeque<TaskRequest> = VecDeque::new();
        let mut dispatched_tasks: HashMap<u64, TaskRequest> = HashMap::new();
        loop {
            let try_dispatch = select! {
                task_req = self.ctx.recv_event::<TaskRequest>().fuse() => {
                    pending_tasks.push_back(task_req.data);
                    pending_tasks.len() == 1
                },
                finished = self.ctx.recv_event::<CompFinished>().fuse() => {
                    let req_id = finished.data.id;
                    let task = dispatched_tasks.remove(&req_id).unwrap();
                    log_debug!(self.ctx, format!("task {} with key {} is completed", task.id, req_id));
                    !pending_tasks.is_empty()
                }
            };
            if try_dispatch {
                for _ in 0..pending_tasks.len() {
                    let task = pending_tasks.front().unwrap();
                    match self.try_dispatch_task(task).await {
                        Some(req_id) => {
                            dispatched_tasks.insert(req_id, pending_tasks.pop_front().unwrap());
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn try_dispatch_task(&self, task: &TaskRequest) -> Option<u64> {
        // pass task to compute and obtain request id used further as event key
        let req_id = self.compute.borrow_mut().run(
            task.flops,
            task.memory,
            task.cores,
            task.cores,
            dslab_compute::multicore::CoresDependency::Linear,
            self.id(),
        ) as EventKey;

        select! {
            _ = self.ctx.recv_event_by_key::<CompStarted>(req_id).fuse() => {
                log_debug!(self.ctx, format!("task {} with key {} is started", task.id, req_id));
                Some(req_id)
            },
            failed = self.ctx.recv_event_by_key::<CompFailed>(req_id).fuse() => {
                log_debug!(self.ctx, format!("task {} with key {} is failed: {}", task.id, req_id, json!(failed.data)));
                None
            }
        }
    }
}

impl StaticEventHandler for Worker {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
        })
    }
}
