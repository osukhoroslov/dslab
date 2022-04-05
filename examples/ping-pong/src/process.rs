use serde::Serialize;

use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;

#[derive(Serialize)]
pub struct Start {}

#[derive(Serialize)]
pub struct Ping {}

#[derive(Serialize)]
pub struct Pong {}

pub struct Process {
    peer_count: usize,
    peers: Vec<Id>,
    iterations: u32,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(peers: Vec<Id>, iterations: u32, ctx: SimulationContext) -> Self {
        Self {
            peer_count: peers.len(),
            peers,
            iterations,
            ctx,
        }
    }

    fn on_start(&mut self) {
        let peer = if self.peer_count > 1 {
            self.peers[self.ctx.gen_range(0..self.peer_count)]
        } else {
            self.peers[0]
        };
        let delay = self.ctx.rand();
        self.ctx.emit(Ping {}, peer, delay);
    }

    fn on_ping(&mut self, from: Id) {
        let delay = self.ctx.rand();
        self.ctx.emit(Pong {}, from, delay);
    }

    fn on_pong(&mut self, from: Id) {
        self.iterations -= 1;
        if self.iterations > 0 {
            let peer = if self.peer_count > 1 {
                self.peers[self.ctx.gen_range(0..self.peer_count)]
            } else {
                from
            };
            let delay = self.ctx.rand();
            self.ctx.emit(Ping {}, peer, delay);
        }
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            Ping {} => {
                self.on_ping(event.src);
            }
            Pong {} => {
                self.on_pong(event.src);
            }
        })
    }
}
