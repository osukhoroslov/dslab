use crate::logger::LogEntry;

#[derive(Debug, Default, PartialEq)]
pub struct McError {
    message: String,
    trace: Vec<LogEntry>,
}

impl McError {
    pub fn new(message: String, trace: Vec<LogEntry>) -> Self {
        Self { message, trace }
    }
}

impl McError {
    pub fn message(&self) -> String {
        self.message.clone()
    }

    pub fn trace(&self) -> Vec<LogEntry> {
        self.trace.clone()
    }
}
