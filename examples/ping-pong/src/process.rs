use serde::Serialize;

use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::log_debug;

#[derive(Serialize)]
pub struct Start {
    other: String,
}

impl Start {
    pub fn new(other: &str) -> Self {
        Self {
            other: other.to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct Ping {}

#[derive(Serialize)]
pub struct Pong {}

pub struct Process {
    iterations: u32,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(iterations: u32, ctx: SimulationContext) -> Self {
        Self { iterations, ctx }
    }

    fn on_start(&mut self, other: &str) {
        let delay = self.ctx.rand();
        self.ctx.emit(Ping {}, other, delay);
    }

    fn on_ping(&mut self, from: String) {
        let delay = self.ctx.rand();
        self.ctx.emit(Pong {}, from, delay);
    }

    fn on_pong(&mut self, from: String) {
        self.iterations -= 1;
        if self.iterations > 0 {
            let delay = self.ctx.rand();
            self.ctx.emit(Ping {}, from, delay);
        }
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start { other } => {
                log_debug!(self.ctx, "received Start from {}", event.src);
                self.on_start(&other);
            }
            Ping {} => {
                log_debug!(self.ctx, "received Ping from {}", event.src);
                self.on_ping(event.src);
            }
            Pong {} => {
                log_debug!(self.ctx, "received Pong from {}", event.src);
                self.on_pong(event.src);
            }
        })
    }
}
