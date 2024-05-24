use std::rc::Rc;

use super::{channel::Receiver, task::Task};

// Polls tasks to advance their state.
// Tasks schedule themselves for polling by writing to the channel which is read by the executor.
pub(crate) struct Executor {
    scheduled_tasks: Receiver<Rc<Task>>,
}

impl Executor {
    // Creates an executor.
    pub fn new(scheduled_tasks: Receiver<Rc<Task>>) -> Self {
        Self { scheduled_tasks }
    }

    // Polls one scheduled task, if any.
    // Returns true if a task was polled and false otherwise.
    pub fn process_task(&self) -> bool {
        if let Some(task) = self.scheduled_tasks.try_recv() {
            task.poll();
            true
        } else {
            false
        }
    }
}
