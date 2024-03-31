//! Queue for producer-consumer communication between asynchronous tasks.

use std::cell::RefCell;
use std::collections::VecDeque;

use serde::Serialize;

use crate::SimulationContext;

/// A simple implementation of unbounded multi-producer multi-consumer queue with blocking receives.
///
/// It can store items of any data type `T`.
/// The items are guarantied to be delivered in the order of [`UnboundedBlockingQueue::receive`] calls.
/// Each future returned by [`UnboundedBlockingQueue::receive`] must be awaited.
pub struct UnboundedBlockingQueue<T> {
    queue: RefCell<VecDeque<T>>,
    send_ticket: Ticket,
    receive_ticket: Ticket,
    ctx: SimulationContext,
}

// TODO: call methods push/pop or rename to channel?
impl<T> UnboundedBlockingQueue<T> {
    pub(crate) fn new(ctx: SimulationContext) -> Self {
        ctx.register_key_getter_for::<ConsumerNotify>(|notify| notify.ticket_id);
        Self {
            queue: RefCell::new(VecDeque::new()),
            send_ticket: Ticket::new(),
            receive_ticket: Ticket::new(),
            ctx,
        }
    }

    /// Send an item to the queue without blocking.
    pub fn send(&self, item: T) {
        self.send_ticket.next();
        self.queue.borrow_mut().push_back(item);
        // notify awaiting consumer if needed
        if self.receive_ticket.is_after(&self.send_ticket) {
            self.ctx.emit_self_now(ConsumerNotify {
                ticket_id: self.send_ticket.value(),
            });
        }
    }

    /// Asynchronously receive an item from the queue.
    pub async fn receive(&self) -> T {
        self.receive_ticket.next();
        // wait for notification from producer side if the queue is empty
        if self.queue.borrow().is_empty() {
            self.ctx
                .recv_event_by_key_from_self::<ConsumerNotify>(self.receive_ticket.value())
                .await;
        }
        self.queue.borrow_mut().pop_front().unwrap()
    }
}

type TicketID = u64;

#[derive(Serialize, Clone)]
struct ConsumerNotify {
    ticket_id: TicketID,
}

struct Ticket {
    value: RefCell<TicketID>,
}

impl Ticket {
    fn new() -> Self {
        Self { value: RefCell::new(0) }
    }

    fn next(&self) {
        *self.value.borrow_mut() += 1;
    }

    fn is_after(&self, other: &Self) -> bool {
        *self.value.borrow() >= *other.value.borrow()
    }

    fn value(&self) -> TicketID {
        *self.value.borrow()
    }
}
