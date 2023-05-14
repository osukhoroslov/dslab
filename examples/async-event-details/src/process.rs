use std::{cell::RefCell, rc::Rc};

use dslab_compute::multicore::{CompFailed, CompFinished, CompStarted, Compute};
use dslab_core::async_core::shared_state::DetailsKey;
use dslab_core::async_core::sync::channel::Channel;
use dslab_core::{cast, log_debug, Event, EventHandler, Id, SimulationContext};

use futures::future::FutureExt;
use futures::select;

use serde::Serialize;
use serde_json::json;

use crate::events::{Start, TaskCompleted, TaskRequest};

#[derive(Serialize)]
pub struct TaskInfo {
    flops: f64,
    memory: u64,
    cores: u32,
}

pub struct Worker {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    compute_id: Id,
    ctx: SimulationContext,
    task_chan: Channel<TaskInfo>,
}

impl Worker {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        compute_id: Id,
        ctx: SimulationContext,
        task_chan: Channel<TaskInfo>,
    ) -> Self {
        Self {
            id: ctx.id(),
            compute,
            compute_id,
            ctx,
            task_chan,
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

        self.task_chan.send(task_info);
    }

    async fn work_loop(&self) {
        let mut tasks_completed = 0;
        loop {
            let task_info = self.task_chan.receive().await;

            while !self.try_start_process_task(&task_info).await {
                self.ctx.async_handle_self::<TaskCompleted>().await;
            }

            tasks_completed += 1;

            log_debug!(self.ctx, format!("work_loop : {} tasks completed", tasks_completed));
        }
    }

    async fn try_start_process_task(&self, task_info: &TaskInfo) -> bool {
        let key = self.run_task(task_info);

        select! {
            _ = self.ctx.async_detailed_handle_event::<CompStarted>(self.compute_id, key).fuse() => {

                log_debug!(self.ctx, format!("try_process_task : task with key {} started", key));

                self.ctx.spawn(self.process_task(key));

                true
            },
            (_, failed) = self.ctx.async_detailed_handle_event::<CompFailed>(self.compute_id, key).fuse() => {
                log_debug!(self.ctx, format!("try_process_task : task with key {} failed: {}", key, json!(failed)));
                false
            }
        }
    }

    async fn process_task(&self, key: DetailsKey) {
        self.ctx
            .async_detailed_handle_event::<CompFinished>(self.compute_id, key)
            .await;

        log_debug!(self.ctx, format!("process_task : task with key {} completed", key));

        self.ctx.emit_self_now(TaskCompleted {});
    }

    fn run_task(&self, task_info: &TaskInfo) -> DetailsKey {
        self.compute.borrow_mut().run(
            task_info.flops,
            task_info.memory,
            task_info.cores,
            task_info.cores,
            dslab_compute::multicore::CoresDependency::Linear,
            self.id(),
        ) as DetailsKey
    }
}

impl EventHandler for Worker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            TaskRequest { flops, cores, memory } => {
                self.on_task_request(TaskInfo { flops, cores, memory });
            }
            Start {} => {
                self.on_start();
            }
            TaskCompleted {} => {}
        })
    }
}
