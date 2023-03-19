use std::collections::{HashMap, HashSet};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;

use crate::mc::events::McEvent::{MessageReceived, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent};
use crate::mc::system::{McState, McSystem};
use crate::message::Message;
use crate::util::t;

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

pub enum VisitedStates {
    Full(HashSet<McState>),
    Partial(HashSet<u64>),
}

#[derive(Debug, Default, Clone)]
pub struct McSummary {
    pub(crate) states: HashMap<String, u32>,
}

pub type PruneFn = Box<dyn Fn(&McState) -> Option<String>>;
pub type GoalFn = Box<dyn Fn(&McState) -> Option<String>>;
pub type InvariantFn = Box<dyn Fn(&McState) -> Result<(), String>>;

pub trait Strategy {
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String>;

    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String>;

    fn process_event(&mut self, system: &mut McSystem, event_num: usize) -> Result<(), String> {
        let event = self.clone_event(system, event_num);
        match event {
            MessageReceived { options, .. } => {
                match options {
                    DeliveryOptions::NoFailures => self.apply_event(system, event_num, false, false)?,
                    DeliveryOptions::Dropped => self.process_drop_event(system, event_num)?,
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    } => {
                        // Drop
                        if can_be_dropped {
                            self.process_drop_event(system, event_num)?;
                        }

                        // Default (normal / corrupt)
                        self.apply_event(system, event_num, false, false)?;
                        if can_be_corrupted {
                            self.apply_event(system, event_num, false, true)?;
                        }

                        // Duplicate (normal / corrupt one)
                        if max_dupl_count > 0 {
                            self.apply_event(system, event_num, true, false)?;
                            if can_be_corrupted {
                                self.apply_event(system, event_num, true, true)?;
                            }
                        }
                    }
                }
            }
            TimerFired { .. } => {
                self.apply_event(system, event_num, false, false)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn process_drop_event(&mut self, system: &mut McSystem, event_num: usize) -> Result<(), String> {
        let state = system.get_state(self.search_depth());
        let event = self.take_event(system, event_num);

        self.debug_log(&event, self.search_depth(), LogContext::Dropped);

        self.search_step_impl(system)?;

        system.set_state(state);

        Ok(())
    }

    fn apply_event(
        &mut self,
        system: &mut McSystem,
        event_num: usize,
        duplicate: bool,
        corrupt: bool,
    ) -> Result<(), String> {
        let state = system.get_state(self.search_depth());

        let mut event;
        if duplicate {
            event = self.duplicate_event(system, event_num);
        } else {
            event = self.take_event(system, event_num);
        }

        if corrupt {
            event = self.corrupt_msg_data(event);
        }

        self.debug_log(&event, self.search_depth(), LogContext::Default);

        system.apply_event(event);

        self.search_step_impl(system)?;

        system.set_state(state);

        Ok(())
    }

    fn take_event(&self, system: &mut McSystem, event_num: usize) -> McEvent {
        system.events.remove(event_num)
    }

    fn clone_event(&self, system: &mut McSystem, event_num: usize) -> McEvent {
        system.events[event_num].clone()
    }

    fn add_event(&self, system: &mut McSystem, event: McEvent, event_num: usize) {
        system.events.insert(event_num, event);
    }

    fn corrupt_msg_data(&self, event: McEvent) -> McEvent {
        self.debug_log(&event, self.search_depth(), LogContext::Corrupted);
        match event {
            MessageReceived {
                msg,
                src,
                dest,
                options,
            } => {
                lazy_static! {
                    static ref RE: Regex = Regex::new(r#""\w+""#).unwrap();
                }
                let corrupted_data = RE.replace_all(&msg.data, "\"\"").to_string();
                MessageReceived {
                    msg: Message::new(msg.tip, corrupted_data),
                    src,
                    dest,
                    options,
                }
            }
            _ => event,
        }
    }

    fn duplicate_event(&self, system: &mut McSystem, event_num: usize) -> McEvent {
        let event = self.take_event(system, event_num);
        self.add_event(system, event.duplicate().unwrap(), event_num);
        self.debug_log(&event, self.search_depth(), LogContext::Duplicated);
        event
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
                _ => {
                    t!(format!("Internal error: unknown event in model checking Strategy").red());
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
