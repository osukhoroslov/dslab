use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;

use crate::mc::events::McEvent::{MessageReceived, TimerCancelled, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent, McEventId};
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

    fn process_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), String> {
        let event = self.clone_event(system, event_id);
        match event {
            MessageReceived { options, .. } => {
                match options {
                    DeliveryOptions::NoFailures(..) => self.apply_event(system, event_id, false, false)?,
                    DeliveryOptions::Dropped => self.process_drop_event(system, event_id)?,
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    } => {
                        // Drop
                        if can_be_dropped {
                            self.process_drop_event(system, event_id)?;
                        }

                        // Default (normal / corrupt)
                        self.apply_event(system, event_id, false, false)?;
                        if can_be_corrupted {
                            self.apply_event(system, event_id, false, true)?;
                        }

                        // Duplicate (normal / corrupt one)
                        if max_dupl_count > 0 {
                            self.apply_event(system, event_id, true, false)?;
                            if can_be_corrupted {
                                self.apply_event(system, event_id, true, true)?;
                            }
                        }
                    }
                }
            }
            TimerFired { .. } => {
                self.apply_event(system, event_id, false, false)?;
            }
            TimerCancelled { .. } => {
                panic!("Strategy should not receive TimerCancelled events")
            }
        }

        Ok(())
    }

    fn process_drop_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), String> {
        let state = system.get_state(self.search_depth());
        let event = self.take_event(system, event_id);

        self.debug_log(&event, self.search_depth(), LogContext::Dropped);

        self.search_step_impl(system)?;

        system.set_state(state);

        Ok(())
    }

    fn apply_event(
        &mut self,
        system: &mut McSystem,
        event_id: McEventId,
        duplicate: bool,
        corrupt: bool,
    ) -> Result<(), String> {
        let state = system.get_state(self.search_depth());

        let mut event;
        if duplicate {
            event = self.duplicate_event(system, event_id);
        } else {
            event = self.take_event(system, event_id);
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

    fn take_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        system.events.pop(event_id)
    }

    fn clone_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        system.events.get(event_id).unwrap().clone()
    }

    fn add_event(&self, system: &mut McSystem, event: McEvent) -> McEventId {
        system.events.push(event)
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

    fn duplicate_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        let event = self.take_event(system, event_id);
        system.events.push_with_fixed_id(event.duplicate().unwrap(), event_id);
        self.debug_log(&event, self.search_depth(), LogContext::Duplicated);
        event
    }

    fn debug_log(&self, event: &McEvent, depth: u64, log_context: LogContext) {
        if self.log_mode() == &LogMode::Debug {
            match event {
                MessageReceived { msg, src, dest, .. } => {
                    self.log_message(depth, msg, src, dest, log_context);
                }
                TimerFired { proc, timer, .. } => {
                    t!(format!("{:>10} | {:>10} !-- {:<10}", depth, proc, timer).yellow());
                }
                _ => {}
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

    fn initialize_visited(log_mode: &LogMode) -> VisitedStates
    where
        Self: Sized,
    {
        match log_mode {
            LogMode::Debug => VisitedStates::Full(HashSet::default()),
            LogMode::Default => VisitedStates::Partial(HashSet::default()),
        }
    }

    fn have_visited(&mut self, state: &McState) -> bool {
        match self.visited() {
            VisitedStates::Full(ref states) => states.contains(state),
            VisitedStates::Partial(ref hashes) => {
                let mut h = DefaultHasher::default();
                state.hash(&mut h);
                hashes.contains(&h.finish())
            }
        }
    }

    fn mark_visited(&mut self, state: McState) {
        match self.visited() {
            VisitedStates::Full(ref mut states) => {
                states.insert(state);
            }
            VisitedStates::Partial(ref mut hashes) => {
                let mut h = DefaultHasher::default();
                state.hash(&mut h);
                hashes.insert(h.finish());
            }
        }
    }

    fn update_summary(&mut self, status: String) {
        if let LogMode::Debug = self.log_mode() {
            let counter = self.summary().states.entry(status).or_insert(0);
            *counter += 1;
        }
    }

    fn check_state(&mut self, state: &McState, events_num: usize) -> Option<Result<(), String>> {
        if self.have_visited(state) {
            // Was already visited before
            Some(Ok(()))
        } else if let Err(err) = (self.invariant())(state) {
            // Invariant is broken
            Some(Err(err))
        } else if let Some(status) = (self.goal())(state) {
            // Reached final state of the system
            self.update_summary(status);
            Some(Ok(()))
        } else if let Some(status) = (self.prune())(state) {
            // Execution branch is pruned
            self.update_summary(status);
            Some(Ok(()))
        } else if events_num == 0 {
            // exhausted without goal completed
            Some(Err("nothing left to do to reach the goal".to_owned()))
        } else {
            None
        }
    }

    fn log_mode(&self) -> &LogMode;

    fn search_depth(&self) -> u64;

    fn visited(&mut self) -> &mut VisitedStates;

    fn prune(&self) -> &PruneFn;

    fn goal(&self) -> &GoalFn;

    fn invariant(&self) -> &InvariantFn;

    fn summary(&mut self) -> &mut McSummary;
}
