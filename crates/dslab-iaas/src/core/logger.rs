/// Logging facilities to record events during simulation.
use std::fs::File;

use log::Level;
use serde::Serialize;

use simcore::SimulationContext;
use simcore::{log_debug, log_error, log_info, log_trace, log_warn};

pub trait Logger {
    fn log_error(&mut self, ctx: &SimulationContext, log: String);

    fn log_warn(&mut self, ctx: &SimulationContext, log: String);

    fn log_info(&mut self, ctx: &SimulationContext, log: String);

    fn log_debug(&mut self, ctx: &SimulationContext, log: String);

    fn log_trace(&mut self, ctx: &SimulationContext, log: String);

    fn save_log(&self, _path: &str) -> Result<(), std::io::Error>;
}

#[derive(Default)]
pub struct StdoutLogger {}

impl Logger for StdoutLogger {
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

    fn save_log(&self, _path: &str) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl StdoutLogger {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Serialize)]
struct LogEntry {
    timestamp: f64,
    component: String,
    message: String,
}

pub struct FileLogger {
    log: Vec<LogEntry>,
    level: Level,
}

impl Default for FileLogger {
    fn default() -> Self {
        Self {
            log: Vec::new(),
            level: Level::Info,
        }
    }
}

impl FileLogger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_level(level: Level) -> Self {
        Self { log: Vec::new(), level }
    }

    fn log_internal(&mut self, ctx: &SimulationContext, message: String, level: Level) {
        if self.level < level {
            return;
        }
        self.log.push(LogEntry {
            timestamp: ctx.time(),
            component: ctx.name().to_string(),
            message,
        });
    }
}

impl Logger for FileLogger {
    fn log_error(&mut self, ctx: &SimulationContext, log: String) {
        self.log_internal(ctx, log, Level::Error)
    }

    fn log_warn(&mut self, ctx: &SimulationContext, log: String) {
        self.log_internal(ctx, log, Level::Warn)
    }

    fn log_info(&mut self, ctx: &SimulationContext, log: String) {
        self.log_internal(ctx, log, Level::Info)
    }

    fn log_debug(&mut self, ctx: &SimulationContext, log: String) {
        self.log_internal(ctx, log, Level::Debug)
    }

    fn log_trace(&mut self, ctx: &SimulationContext, log: String) {
        self.log_internal(ctx, log, Level::Trace)
    }

    fn save_log(&self, path: &str) -> Result<(), std::io::Error> {
        let file = File::create(path)?;
        let mut wtr = csv::Writer::from_writer(file);
        for entry in &self.log {
            wtr.serialize(entry)?;
        }
        wtr.flush()?;
        Ok(())
    }
}
