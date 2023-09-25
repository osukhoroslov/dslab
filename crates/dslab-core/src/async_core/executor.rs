//! Executing tasks from ready_queue.

use std::{rc::Rc, sync::mpsc::Receiver, task::Context};

use super::{task::Task, waker};

/// Polling tasks from queue.
pub struct Executor {
    ready_queue: Receiver<Rc<Task>>,
}

impl Executor {
    /// Creates an executor.
    pub fn new(ready_queue: Receiver<Rc<Task>>) -> Self {
        Self { ready_queue }
    }

    /// Polls one task from ready_queue.
    ///
    /// Returns true if any progress has been made, false otherwise.
    pub fn process_task(&self) -> bool {
        if let Ok(task) = self.ready_queue.try_recv() {
            let mut future_slot = task.future.borrow_mut();

            if let Some(mut future) = future_slot.take() {
                // Create a `LocalWaker` from the task itself
                let waker = waker::waker_ref(&task);

                let context = &mut Context::from_waker(&waker);

                if future.as_mut().poll(context).is_pending() {
                    *future_slot = Some(future);
                }

                return true;
            }
        }

        false
    }
}
