use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;

use super::channel::Sender;
use super::waker::{waker_ref, RcWake};

type BoxedFuture = Pin<Box<dyn Future<Output = ()>>>;

// Represents an asynchronous task spawned via Simulation::spawn or SimulationContext::spawn.
// Holds the corresponding future and schedules itself for polling by Executor on wake-up notifications.
pub(crate) struct Task {
    future: RefCell<Option<BoxedFuture>>,
    executor: Sender<Rc<Task>>,
}

impl Task {
    // Creates a new task from a future.
    fn new(future: impl Future<Output = ()> + 'static, executor: Sender<Rc<Task>>) -> Self {
        Self {
            future: RefCell::new(Some(Box::pin(future))),
            executor,
        }
    }

    // Converts a future into a task and sends it to executor.
    pub fn spawn(future: impl Future<Output = ()> + 'static, executor: Sender<Rc<Task>>) {
        let task = Rc::new(Task::new(future, executor));
        task.schedule();
    }

    // Polls the internal future and passes waker to it.
    // This method is called by the executor when the task is created or woken up.
    // Calling this method after the task completion will result in panic.
    pub fn poll(self: Rc<Self>) {
        let mut future_slot = self.future.borrow_mut();
        if let Some(mut future) = future_slot.take() {
            // Create a waker from the task itself
            let waker = waker_ref(&self);
            // Create async context with waker and poll future with it
            let async_ctx = &mut Context::from_waker(&waker);
            if future.as_mut().poll(async_ctx).is_pending() {
                // Keep storing pending future
                *future_slot = Some(future);
            }
        } else {
            panic!("Task is polled after completion")
        }
    }

    // Schedules the task for polling by sending it to the executor.
    fn schedule(self: &Rc<Self>) {
        self.executor.send(self.clone());
    }
}

impl RcWake for Task {
    fn wake_by_ref(rc_self: &Rc<Self>) {
        rc_self.schedule();
    }
}
