use crate::{task::Task, waker};
use std::sync::{mpsc::Receiver, Arc};
use std::task::Context;

pub struct Executor {
    pub ready_queue: Receiver<Arc<Task>>,
}

impl Executor {
    pub fn process_task(&self) -> bool {
        if let Ok(task) = self.ready_queue.try_recv() {
            let mut future_slot = task.future.borrow_mut();

            if let Some(mut future) = future_slot.take() {
                // Create a `LocalWaker` from the task itself
                let waker = waker::waker_ref(&task);

                let context = &mut Context::from_waker(&*waker);

                if future.as_mut().poll(context).is_pending() {
                    *future_slot = Some(future);
                }

                return true;
            }
        }

        return false;
    }
}
