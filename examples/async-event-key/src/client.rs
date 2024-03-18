use dslab_core::{Event, EventHandler, Id, SimulationContext};

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

    pub fn run(&self) {
        self.ctx.spawn(self.submit_tasks())
    }

    async fn submit_tasks(&self) {
        for _ in 0..self.task_count {
            // submit new task
            let cores = self.ctx.gen_range(1..=8);
            let memory = self.ctx.gen_range(1..=4) * 1024_u64;
            let flops = self.ctx.gen_range(1..=3000) as f64;
            self.ctx.emit_now(TaskRequest { cores, memory, flops }, self.worker_id);

            // sleep with random delay
            self.ctx.sleep(self.ctx.gen_range(1.0..=self.max_task_delay)).await;
        }
    }
}

// Empty event handler is needed because spawning async tasks by a component without event handler is prohibited
impl EventHandler for Client {
    fn on(&mut self, _event: Event) {}
}
