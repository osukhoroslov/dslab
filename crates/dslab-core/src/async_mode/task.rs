use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::task::Context;

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
    //
    // Unsafe is used to make possible spawning components' methods as tasks via SimulationContext::spawn.
    // &self argument prevents any method to have a 'static lifetime, but following the simulation logic
    // the spawned tasks should always finish before the corresponding components are deleted:
    // - deletion of components is supposed to be done only through the Simulation::remove_handler method,
    // - components are not supposed to be moved because they are allocated in the heap under Rc<RefCell<...>>.
    fn new(future: impl Future<Output = ()>, executor: Sender<Rc<Task>>) -> Self {
        unsafe {
            let boxed: Box<dyn Future<Output = ()>> = Box::new(future);
            let converted: Box<dyn Future<Output = ()> + 'static> = std::mem::transmute(boxed);
            Self {
                future: RefCell::new(Some(Box::into_pin(converted))),
                executor,
            }
        }
    }

    // Converts a future into a task and sends it to executor.
    pub fn spawn(future: impl Future<Output = ()>, executor: Sender<Rc<Task>>) {
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
        self.executor.send(self.clone()).expect("channel is closed");
    }
}

impl RcWake for Task {
    fn wake_by_ref(rc_self: &Rc<Self>) {
        rc_self.schedule();
    }
}
