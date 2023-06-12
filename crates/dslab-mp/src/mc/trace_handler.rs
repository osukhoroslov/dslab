use crate::logger::LogEntry;

#[derive(Default)]
pub struct TraceHandler {
    trace: Vec<LogEntry>,
}

impl TraceHandler {
    pub fn push(&mut self, entry: LogEntry) {
        self.trace.push(entry);
    }

    pub fn trace(&self) -> Vec<LogEntry> {
        self.trace.clone()
    }

    pub fn set_trace(&mut self, trace: Vec<LogEntry>) {
        self.trace = trace;
    }
}
