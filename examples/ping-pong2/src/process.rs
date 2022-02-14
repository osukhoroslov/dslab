use log::debug;

use core2::cast;
use core2::context::SimulationContext;
use core2::event::Event;
use core2::handler::EventHandler;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Ping {}

#[derive(Debug)]
pub struct Pong {}

pub struct Process {
    id: String,
    iterations: u32,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(iterations: u32, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id().to_string(),
            iterations,
            ctx,
        }
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
                debug!("{:.2} [{}] received Start from {}", self.ctx.time(), self.id, event.src);
                self.on_start(other);
            }
            Ping {} => {
                debug!("{:.2} [{}] received Ping from {}", self.ctx.time(), self.id, event.src);
                self.on_ping(event.src);
            }
            Pong {} => {
                debug!("{:.2} [{}] received Pong from {}", self.ctx.time(), self.id, event.src);
                self.on_pong(event.src);
            }
        })
    }
}
