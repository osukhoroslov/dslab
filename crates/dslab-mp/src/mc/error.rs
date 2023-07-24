//! Model checking error.

use crate::logger::LogEntry;

/// Stores information about an error found by model checking.
#[derive(Debug, Default, PartialEq)]
pub struct McError {
    message: String,
    trace: Vec<LogEntry>,
}

impl McError {
    /// Creates new model checking error.
    pub fn new(message: String, trace: Vec<LogEntry>) -> Self {
        Self { message, trace }
    }
}

impl McError {
    /// Returns the error message.
    pub fn message(&self) -> String {
        self.message.clone()
    }

    /// Returns the execution trace which led the system to erroneous state.
    pub fn trace(&self) -> &Vec<LogEntry> {
        &self.trace
    }
}
