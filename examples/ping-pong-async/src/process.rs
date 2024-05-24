use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::handler::StaticEventHandler;
use serde::Serialize;

use dslab_core::cast;
use dslab_core::event::EventData;
use dslab_core::{Event, Id, SimulationContext};
use dslab_network::Network;

#[derive(Clone, Serialize)]
pub struct Start {}

#[derive(Clone, Serialize)]
pub struct Ping {
    payload: f64,
}

#[derive(Clone, Serialize)]
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

    fn on_start(self: Rc<Self>) {
        if self.is_pinger {
            self.ctx.spawn(self.clone().pinger_loop());
        }
    }

    async fn pinger_loop(self: Rc<Self>) {
        for _ in 0..self.iterations {
            let peer = if self.peer_count > 1 {
                self.peers[self.ctx.gen_range(0..self.peer_count)]
            } else {
                self.peers[0]
            };
            self.send(
                Ping {
                    payload: self.ctx.time(),
                },
                peer,
            );
            self.ctx.recv_event_from::<Pong>(peer).await;
        }
    }

    fn on_ping(&self, from: Id) {
        self.send(
            Pong {
                payload: self.ctx.time(),
            },
            from,
        );
    }

    fn send<T: EventData>(&self, event: T, to: Id) {
        let delay = if self.rand_delay { self.ctx.rand() } else { 1. };
        self.ctx.emit(event, to, delay);
    }
}

impl StaticEventHandler for Process {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            Ping { payload: _ } => {
                self.on_ping(event.src);
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

    fn on_start(self: Rc<Self>) {
        if self.is_pinger {
            self.ctx.spawn(self.clone().pinger_loop());
        }
    }

    pub async fn pinger_loop(self: Rc<Self>) {
        for _ in 0..=self.iterations {
            let peer = if self.peer_count > 1 {
                self.peers[self.ctx.gen_range(0..self.peer_count)]
            } else {
                self.peers[0]
            };
            self.net.borrow_mut().send_event(
                Ping {
                    payload: self.ctx.time(),
                },
                self.id,
                peer,
            );
            self.ctx.recv_event_from::<Pong>(peer).await;
        }
    }

    fn on_ping(&self, from: Id) {
        self.net.borrow_mut().send_event(
            Pong {
                payload: self.ctx.time(),
            },
            self.id,
            from,
        );
    }
}

impl StaticEventHandler for NetworkProcess {
    fn on(self: Rc<Self>, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.on_start();
            }
            Ping { payload: _ } => {
                self.on_ping(event.src);
            }
        })
    }
}
