use std::{cell::RefCell, rc::Rc};

use futures::future::FutureExt;
use futures::select;
use serde::Serialize;
use serde_json::json;

use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted, Compute};
use dslab_core::async_mode::await_details::EventKey;
use dslab_core::async_mode::sync::queue::UnboundedBlockingQueue;
use dslab_core::{cast, log_debug, Event, EventHandler, Id, SimulationContext};

use crate::events::{Start, TaskCompleted, TaskRequest};

#[derive(Serialize)]
pub struct TaskInfo {
    cores: u32,
    memory: u64,
    flops: f64,
}

pub struct Worker {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    task_queue: UnboundedBlockingQueue<TaskInfo>,
    ctx: SimulationContext,
}

impl Worker {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        task_queue: UnboundedBlockingQueue<TaskInfo>,
        ctx: SimulationContext,
    ) -> Self {
        // register key getters for compute events
        ctx.register_key_getter_for::<CompStarted>(|e| e.id);
        ctx.register_key_getter_for::<CompFailed>(|e| e.id);
        ctx.register_key_getter_for::<CompFinished>(|e| e.id);
        Self {
            id: ctx.id(),
            compute,
            task_queue,
            ctx,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    fn on_start(&self) {
        log_debug!(self.ctx, "Worker started");
        self.ctx.spawn(self.work_loop());
    }

    fn on_task_request(&self, task_info: TaskInfo) {
        log_debug!(self.ctx, format!("Received task: {}", json!(&task_info)));
        self.task_queue.send(task_info);
    }

    async fn work_loop(&self) {
        let mut tasks_dispatched = 0;
        loop {
            let task_info = self.task_queue.receive().await;
            while !self.try_dispatch_task(&task_info).await {
                self.ctx.recv_event_from_self::<TaskCompleted>().await;
            }
            tasks_dispatched += 1;
            log_debug!(self.ctx, format!("work_loop : {} tasks dispatched", tasks_dispatched));
        }
    }

    async fn try_dispatch_task(&self, task_info: &TaskInfo) -> bool {
        // pass task to compute and obtain request id used further as event key
        let req_id = self.compute.borrow_mut().run(
            task_info.flops,
            task_info.memory,
            task_info.cores,
            task_info.cores,
            dslab_compute::multicore::CoresDependency::Linear,
            self.id(),
        ) as EventKey;

        select! {
            _ = self.ctx.recv_event_by_key::<CompStarted>(req_id).fuse() => {
                log_debug!(self.ctx, format!("try_dispatch_task : task with key {} is started", req_id));
                self.ctx.spawn(self.track_task_state(req_id));
                true
            },
            (_, failed) = self.ctx.recv_event_by_key::<CompFailed>(req_id).fuse() => {
                log_debug!(self.ctx, format!("try_dispatch_task : task with key {} is failed: {}", req_id, json!(failed)));
                false
            }
        }
    }

    async fn track_task_state(&self, req_id: EventKey) {
        self.ctx.recv_event_by_key::<CompFinished>(req_id).await;
        log_debug!(
            self.ctx,
            format!("track_task_state : task with key {} is completed", req_id)
        );
        self.ctx.emit_self_now(TaskCompleted {});
    }
}

impl EventHandler for Worker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            TaskRequest { cores, memory, flops } => {
                self.on_task_request(TaskInfo { cores, memory, flops });
            }
            Start {} => {
                self.on_start();
            }
            TaskCompleted {} => {}
        })
    }
}
