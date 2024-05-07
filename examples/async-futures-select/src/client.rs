use std::rc::Rc;

use dslab_core::{Event, Id, SharedEventHandler, SimulationContext};

use crate::events::TaskRequest;

pub struct Client {
    ctx: SimulationContext,
    task_count: u32,
    max_task_delay: f64,
    worker_id: Id,
}

impl Client {
    pub fn new(ctx: SimulationContext, task_count: u32, max_task_delay: f64, worker_id: Id) -> Self {
        Self {
            ctx,
            task_count,
            max_task_delay,
            worker_id,
        }
    }

    pub fn run(self: Rc<Self>) {
        self.ctx.spawn(self.clone().submit_tasks())
    }

    async fn submit_tasks(self: Rc<Self>) {
        for i in 0..self.task_count {
            // submit new task
            let cores = self.ctx.gen_range(1..=8);
            let memory = self.ctx.gen_range(1..=4) * 1024_u64;
            let flops = self.ctx.gen_range(1..=3000) as f64;
            self.ctx.emit_now(
                TaskRequest {
                    id: i as u64,
                    cores,
                    memory,
                    flops,
                },
                self.worker_id,
            );

            // sleep with random delay
            self.ctx.sleep(self.ctx.gen_range(1.0..=self.max_task_delay)).await;
        }
    }
}

// Empty event handler is needed because spawning async tasks by a component without event handler is prohibited
impl SharedEventHandler for Client {
    fn on(self: Rc<Self>, _event: Event) {}
}
