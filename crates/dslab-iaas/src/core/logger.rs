/// Simulation logger where log are configured
use std::fs::File;
use std::io::Write;

use dslab_core::SimulationContext;
use dslab_core::{log_debug, log_error, log_info, log_trace, log_warn};

pub trait Logger {
    fn log_error(&mut self, ctx: &SimulationContext, log: String) {
        log_error!(ctx, log);
    }

    fn log_warn(&mut self, ctx: &SimulationContext, log: String) {
        log_warn!(ctx, log);
    }

    fn log_info(&mut self, ctx: &SimulationContext, log: String) {
        log_info!(ctx, log);
    }

    fn log_debug(&mut self, ctx: &SimulationContext, log: String) {
        log_debug!(ctx, log);
    }

    fn log_trace(&mut self, ctx: &SimulationContext, log: String) {
        log_trace!(ctx, log);
    }

    fn save_to_file(&self, _filename: &str) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub struct StdoutLogger {}

impl Logger for StdoutLogger {}

impl StdoutLogger {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StdoutLogger {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TraceLogger {
    trace: Vec<String>,
}

impl Logger for TraceLogger {
    fn log_error(&mut self, _ctx: &SimulationContext, log: String) {
        self.trace.push(log);
    }

    fn log_warn(&mut self, _ctx: &SimulationContext, log: String) {
        self.trace.push(log);
    }

    fn log_info(&mut self, _ctx: &SimulationContext, log: String) {
        self.trace.push(log);
    }

    fn log_debug(&mut self, _ctx: &SimulationContext, log: String) {
        self.trace.push(log);
    }

    fn log_trace(&mut self, _ctx: &SimulationContext, log: String) {
        self.trace.push(log);
    }

    fn save_to_file(&self, filename: &str) -> Result<(), std::io::Error> {
        File::create(filename)
            .unwrap()
            .write_all(serde_json::to_string_pretty(&self.trace).unwrap().as_bytes())
    }
}

impl TraceLogger {
    pub fn new() -> Self {
        Self { trace: Vec::new() }
    }
}

impl Default for TraceLogger {
    fn default() -> Self {
        Self::new()
    }
}
