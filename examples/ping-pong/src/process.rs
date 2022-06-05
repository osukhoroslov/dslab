use serde::Serialize;
use std::cell::RefCell;
use std::rc::Rc;

use network::network::Network;
use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::{Event, EventData};
use simcore::handler::EventHandler;

#[derive(Serialize)]
pub struct Start {}

#[derive(Serialize)]
pub struct Ping {
    payload: f64,
}

#[derive(Serialize)]
pub struct Pong {
    payload: f64,
}

pub struct Process {
    peer_count: usize,
    peers: Vec<Id>,
    is_pinger: bool,
    rand_delay: bool,
    iterations: u32,
    ctx: SimulationContext,
}

impl Process {
    pub fn new(peers: Vec<Id>, is_pinger: bool, rand_delay: bool, iterations: u32, ctx: SimulationContext) -> Self {
        Self {
            peer_count: peers.len(),
            peers,
            is_pinger,
            rand_delay,
            iterations,
            ctx,
        }
    }

    fn on_start(&mut self) {
        if self.is_pinger {
            let peer = self.peers[self.ctx.gen_range(0..self.peer_count)];
            self.send(
                Ping {
                    payload: self.ctx.time(),
                },
                peer,
            );
        }
    }

    fn on_ping(&mut self, from: Id) {
        self.send(
            Pong {
                payload: self.ctx.time(),
            },
            from,
        );
    }

    fn on_pong(&mut self, from: Id) {
        self.iterations -= 1;
        if self.iterations > 0 {
            let peer = if self.peer_count > 1 {
                self.peers[self.ctx.gen_range(0..self.peer_count)]
            } else {
                from
            };
            self.send(
                Ping {
                    payload: self.ctx.time(),
                },
                peer,
            );
        }
    }

    fn send<T: EventData>(&mut self, event: T, to: Id) {
        let delay = if self.rand_delay { self.ctx.rand() } else { 1. };
        self.ctx.emit(event, to, delay);
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            Ping { payload: _ } => {
                self.on_ping(event.src);
            }
            Pong { payload: _ } => {
                self.on_pong(event.src);
            }
        })
    }
}

pub struct NetworkProcess {
    id: Id,
    peer_count: usize,
    peers: Vec<Id>,
    is_pinger: bool,
    iterations: u32,
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl NetworkProcess {
    pub fn new(
        peers: Vec<Id>,
        is_pinger: bool,
        iterations: u32,
        net: Rc<RefCell<Network>>,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            peer_count: peers.len(),
            peers,
            is_pinger,
            iterations,
            net,
            ctx,
        }
    }

    fn on_start(&mut self) {
        if self.is_pinger {
            let peer = self.peers[self.ctx.gen_range(0..self.peer_count)];
            self.net.borrow_mut().send_event(
                Ping {
                    payload: self.ctx.time(),
                },
                self.id,
                peer,
            );
        }
    }

    fn on_ping(&mut self, from: Id) {
        self.net.borrow_mut().send_event(
            Pong {
                payload: self.ctx.time(),
            },
            self.id,
            from,
        );
    }

    fn on_pong(&mut self, from: Id) {
        self.iterations -= 1;
        if self.iterations > 0 {
            let peer = if self.peer_count > 1 {
                self.peers[self.ctx.gen_range(0..self.peer_count)]
            } else {
                from
            };
            self.net.borrow_mut().send_event(
                Ping {
                    payload: self.ctx.time(),
                },
                self.id,
                peer,
            );
        }
    }
}

impl EventHandler for NetworkProcess {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            Ping { payload: _ } => {
                self.on_ping(event.src);
            }
            Pong { payload: _ } => {
                self.on_pong(event.src);
            }
        })
    }
}
