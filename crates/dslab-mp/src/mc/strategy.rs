use colored::*;

use crate::mc::events::McEvent;
use crate::mc::events::McEvent::{MessageReceived, TimerFired};
use crate::mc::system::McSystem;
use crate::message::Message;
use crate::util::t;
use std::collections::HashMap;

#[derive(Clone, PartialEq)]
pub enum LogMode {
    Default,
    Debug,
}

pub enum LogContext {
    Default,
    Dropped,
    Duplicated,
    Corrupted,
}

#[derive(Debug, Default, Clone)]
pub struct McSummary {
    pub(crate) states: HashMap<String, u32>,
}

pub trait Strategy {
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String>;

    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String>;

    fn drop_impl(&mut self, system: &mut McSystem) -> Result<(), String>;

    fn process_event(&mut self, system: &mut McSystem, event_num: usize) -> Result<(), String> {
        let event = system.events.borrow()[event_num].clone();
        match event {
            MessageReceived {
                msg: _msg,
                src: _src,
                dest: _dest,
                can_be_dropped,
                ..
            } => {
                if can_be_dropped {
                    if let Err(err) = self.process_drop_event(system, event_num) {
                        return Err(err);
                    }
                }
            }

            TimerFired { .. } => {}
        }

        if let Err(err) = self.apply_event(system, event_num) {
            return Err(err);
        }

        Ok(())
    }

    fn process_drop_event(&mut self, system: &mut McSystem, event_num: usize) -> Result<(), String>;

    fn apply_event(&mut self, system: &mut McSystem, event_num: usize) -> Result<(), String> {
        let state = system.get_state(self.search_depth());
        let event = system.events.borrow_mut().remove(event_num);

        self.debug_log(&event, self.search_depth(), LogContext::Default);

        system.apply_event(event);

        let run_success = self.search_step_impl(system);

        if let Err(err) = run_success {
            return Err(err);
        }

        system.set_state(state);

        Ok(())
    }

    fn debug_log(&self, event: &McEvent, depth: u64, log_context: LogContext) {
        if self.log_mode() == &LogMode::Debug {
            match event {
                MessageReceived { msg, src, dest, .. } => {
                    self.log_message(depth, msg, src, dest, log_context);
                }
                TimerFired { proc, timer } => {
                    t!(format!("{:>10} | {:>10} !-- {:<10}", depth, proc, timer).yellow());
                }
            }
        }
    }

    fn log_message(&self, depth: u64, msg: &Message, src: &String, dest: &String, log_context: LogContext) {
        match log_context {
            LogContext::Default => {
                t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dest, src, msg);
            }
            LogContext::Dropped => {
                t!(format!(
                    "{:>9} {:>10} --x {:<10} {:?} <-- message dropped",
                    "!!!", src, dest, msg
                )
                .red());
            }
            LogContext::Duplicated => {
                t!(format!(
                    "{:>9} {:>10} -=≡ {:<10} {:?} <-- message duplicated",
                    "~~~", src, dest, msg
                )
                .blue());
            }
            LogContext::Corrupted => {
                t!(format!("{:?} <-- message corrupted", msg).red());
            }
        }
    }

    fn log_mode(&self) -> &LogMode;

    fn search_depth(&self) -> u64;
}
