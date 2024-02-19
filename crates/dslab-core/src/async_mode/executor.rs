//! Executing tasks from ready_queue.

use std::{rc::Rc, sync::mpsc::Receiver};

use super::task::Task;

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
            task.poll();
            true
        } else {
            false
        }
    }
}
