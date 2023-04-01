use std::{cell::RefCell, pin::Pin, sync::mpsc::SyncSender, sync::Arc};

use crate::waker::CustomWake;
use std::future::Future;

pub struct Task {
    pub future: RefCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,

    task_sender: SyncSender<Arc<Task>>,
}

impl Task {
    pub fn new(future: impl Future<Output = ()> + 'static, task_sender: SyncSender<Arc<Task>>) -> Self {
        Self {
            future: RefCell::new(Some(Box::pin(future))),
            task_sender,
        }
    }
}

impl CustomWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let cloned = arc_self.clone();
        arc_self.task_sender.send(cloned).expect("too many tasks queued");
    }
}
