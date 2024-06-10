//! Components used in the profiling example.

use serde::Serialize;

use dslab_core::{cast, Event, EventHandler, Id, SimulationContext};

const TOTAL_QUEUE_TIME: f64 = 1e5;

/// A message that is emitted by the server.
#[derive(Clone, Serialize)]
pub struct Message {}

/// A server component that emits messages to clients.
pub struct Server {
    clients: Vec<Id>,
    events_count: u64,
    emit_ordered: bool,
    rand_client_choose: bool,
    ctx: SimulationContext,
}

impl Server {
    /// Create a new server component.
    pub fn new(
        ctx: SimulationContext,
        clients: Vec<Id>,
        events_count: u64,
        emit_ordered: bool,
        rand_client_choose: bool,
    ) -> Self {
        Self {
            clients,
            events_count,
            emit_ordered,
            rand_client_choose,
            ctx,
        }
    }

    /// Start the server by emitting messages to clients.
    ///
    /// The messages are emitted in a round-robin fashion to the clients or
    /// randomly if `rand_client_choose` is set to `true`.
    ///
    /// Based on the `emit_ordered` flag uses either `emit` or `emit_ordered` method
    /// to compare the performance of the two.
    pub fn start(&self) {
        let mut next_client = 0;
        let mut delay = 1.;
        let delay_step = TOTAL_QUEUE_TIME / self.events_count as f64;

        for _ in 0..self.events_count {
            let client_to_message = if self.rand_client_choose {
                self.ctx.gen_range(0..self.clients.len())
            } else {
                next_client = (next_client + 1) % self.clients.len();
                next_client
            };

            if self.emit_ordered {
                self.ctx
                    .emit_ordered(Message {}, self.clients[client_to_message], delay);
            } else {
                self.ctx.emit(Message {}, self.clients[client_to_message], delay);
            }

            delay += delay_step;
        }
    }
}

/// A client component that counts the number of messages received.
#[derive(Default)]
pub struct Client {
    messages_received: u64,
}

impl Client {
    /// Get the number of messages received by the client.
    pub fn messages_count(&self) -> u64 {
        self.messages_received
    }
}

impl EventHandler for Client {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Message {} => {
                self.messages_received += 1
            }
        });
    }
}
