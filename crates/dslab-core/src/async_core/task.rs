//! task declaration

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{mpsc::Sender, Arc},
};

use super::waker::CustomWake;

/// Abstract task that contains future and sends itself to the executor on
/// wake-up notification
pub struct Task {
    /// future to be polled by executor
    pub future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,

    task_sender: Sender<Arc<Task>>,
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
    pub fn new(future: impl Future<Output = ()>, task_sender: Sender<Arc<Task>>) -> Self {
        unsafe {
            let boxed: Box<dyn Future<Output = ()>> = Box::new(future);
            let converted: Box<dyn Future<Output = ()> + 'static> = std::mem::transmute(boxed);
            Self {
                future: RefCell::new(Some(Box::into_pin(converted))),
                task_sender,
            }
        }
    }
}

impl CustomWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let cloned = arc_self.clone();
        arc_self.task_sender.send(cloned).expect("channel is closed");
    }
}
