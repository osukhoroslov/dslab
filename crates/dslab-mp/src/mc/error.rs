//! Model checking error.

use std::fmt::Display;

use crate::{logger::LogEntry, util::t};

/// Stores information about an error found by model checking.
#[derive(PartialEq)]
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

    /// Prints error information required for debugging
    pub fn print(&self) {
        t!("\n\n===\n");
        for log in &self.trace {
            log.print();
        }
    }
}

impl Display for McError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
