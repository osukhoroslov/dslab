//! Task declaration.

use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, sync::mpsc::Sender, task::Context};

use super::waker::{waker_ref, RcWake};

/// Abstract task that contains future and sends itself to the executor on
/// wake-up notification
pub struct Task {
    /// future to be polled by executor
    pub future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,

    executor: Sender<Rc<Task>>,
}

impl Task {
    /// Creates a new task from future.
    ///
    /// Unsafe is required here to make possible spawning components methods as tasks.
    /// &self as argument prevents any method to have 'static lifetime, but following the simulation
    /// logic tasks always should finish before components are deleted:
    /// - Deleting components are supposed to be done only through the Simulation::remove_handler method.
    /// - Components are not supposed to be moved because they are located in the Heap memory under Rc<RefCell<...>>
    ///
    /// TODO: implement task cancellation to increase bug-safety of user space code.
    ///
    pub fn new(future: impl Future<Output = ()>, executor: Sender<Rc<Task>>) -> Self {
        unsafe {
            let boxed: Box<dyn Future<Output = ()>> = Box::new(future);
            let converted: Box<dyn Future<Output = ()> + 'static> = std::mem::transmute(boxed);
            Self {
                future: RefCell::new(Some(Box::into_pin(converted))),
                executor,
            }
        }
    }

    /// Polls the task.
    ///
    /// This method is called by the executor when the task is woken up.
    pub fn poll(self: Rc<Self>) {
        let mut future_slot = self.future.borrow_mut();

        if let Some(mut future) = future_slot.take() {
            // Create a waker from the task itself
            let waker = waker_ref(&self);

            let context = &mut Context::from_waker(&waker);

            if future.as_mut().poll(context).is_pending() {
                *future_slot = Some(future);
            }
        } else {
            panic!("internal error: task polled after completion")
        }
    }

    /// Sends the task to the executor.
    pub fn schedule(self: &Rc<Self>) {
        self.executor.send(self.clone()).expect("channel is closed");
    }

    /// Converts the future into a task and sends it to the executor.
    pub fn spawn(future: impl Future<Output = ()>, executor: Sender<Rc<Task>>) {
        let task = Rc::new(Task::new(future, executor));
        task.schedule();
    }
}

impl RcWake for Task {
    fn wake_by_ref(rc_self: &Rc<Self>) {
        rc_self.schedule();
    }
}
