//! task declaration

use std::{
    cell::RefCell,
    pin::Pin,
    sync::{mpsc::Sender, Arc},
};

use super::waker::CustomWake;
use std::future::Future;

/// Abstract task that contains future and sends itself to the executor on
/// wake-up notification
pub struct Task {
    /// future to be polled by executor
    pub future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,

    task_sender: Sender<Arc<Task>>,
}

impl Task {
    /// Create a new task from future. TODO explain unsafe
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
