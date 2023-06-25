//! Implementation of model checking error struct

use crate::logger::LogEntry;

/// Model checking error struct
#[derive(Debug, Default, PartialEq)]
pub struct McError {
    message: String,
    trace: Vec<LogEntry>,
}

impl McError {
    /// Create new model checking error
    pub fn new(message: String, trace: Vec<LogEntry>) -> Self {
        Self { message, trace }
    }
}

impl McError {
    /// Get model checking error message
    pub fn message(&self) -> String {
        self.message.clone()
    }

    /// Get trace which led system to the error
    pub fn trace(&self) -> &Vec<LogEntry> {
        &self.trace
    }
}
