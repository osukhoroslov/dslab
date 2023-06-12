use crate::logger::LogEntry;

#[derive(Debug, Default, PartialEq)]
pub struct McError {
    str: String,
    trace: Vec<LogEntry>,
}

impl McError {
    pub fn new(str: String, trace: Vec<LogEntry>) -> Self {
        Self { str, trace }
    }
}

impl McError {
    pub fn str(&self) -> String {
        self.str.clone()
    }

    pub fn trace(&self) -> Vec<LogEntry> {
        self.trace.clone()
    }
}
