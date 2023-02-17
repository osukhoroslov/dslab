use colored::*;

use crate::mc::events::McEvent;
use crate::mc::events::McEvent::{MessageReceived, TimerFired};
use crate::mc::system::McSystem;
use crate::util::t;

#[derive(Clone, PartialEq)]
pub enum LogMode {
    Default,
    Debug,
}

pub trait Strategy {
    fn run(&mut self, system: &mut McSystem) -> bool;

    fn debug_log(&self, event: &McEvent, depth: u64) {
        if self.log_mode() == &LogMode::Debug {
            match event {
                MessageReceived { msg, src, dest, .. } => {
                    t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dest, src, msg);
                }
                TimerFired { proc, timer } => {
                    t!(format!("{:>10} | {:>10} !-- {:<10}", depth, proc, timer).yellow());
                }
            }
        }
    }

    fn log_mode(&self) -> &LogMode;
}
