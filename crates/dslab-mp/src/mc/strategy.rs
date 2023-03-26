//! Main logic and configuration of model checking strategy.

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

/// Defines the situation in which the model checking algorithm is executing.
#[derive(Clone, PartialEq)]
pub enum ExecutionMode {
    /// Fast and efficient production execution with truncated execution info.
    Default,

    /// More slow execution with informative additional execution info.
    Debug,
}

/// Status of the message which is passed to logging.
pub enum LogContext {
    /// Nothing happened to the message.
    Default,

    /// Message was dropped.
    Dropped,

    /// Message was duplicated.
    Duplicated,

    /// Message data was corrupted.
    Corrupted,
}

/// Mode of checking if the state of the system was previously visited.
pub enum VisitedStates {
    /// Checking for equality of states (slow).
    Full(HashSet<McState>),

    /// Checking for equality of hashes of states (fast, but with rare collisions).
    Partial(HashSet<u64>),
}

/// Trait with common functionality for different model checking search strategies.
#[derive(Debug, Default, Clone)]
pub struct McSummary {
    pub(crate) states: HashMap<String, u32>,
}

/// Checks if execution branch in system states graph should be pruned.
/// Called for particular state. If returns true, no child states of the considered state would be checked.
pub type PruneFn = Box<dyn Fn(&McState) -> Option<String>>;

/// Checks if the system state is the final state (that is, all significant events in the system have already occurred).
pub type GoalFn = Box<dyn Fn(&McState) -> Option<String>>;

/// Invariant which is checking correctness of execution.
/// If returns false, model checking found error in user algorithm.
pub type InvariantFn = Box<dyn Fn(&McState) -> Result<(), String>>;

/// Trait with common functions for different model checking strategies.
pub trait Strategy {
    /// Launches the strategy execution.
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String>;

    /// Chooses the next system state to consider (the part in which the strategies differ).
    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String>;

    /// Entrypoint to processing system event.
    /// Explores possible outcomes according to event type and options.
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

    /// Processes event dropping.
    fn process_drop_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), String> {
        let state = system.get_state(self.search_depth());
        let event = self.take_event(system, event_id);

        self.debug_log(&event, self.search_depth(), LogContext::Dropped);

        self.search_step_impl(system)?;

        system.set_state(state);

        Ok(())
    }

    /// Initiates applying event to the system and restores the system state afterwards.
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

    /// Takes the event from pending events by id.
    fn take_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        system.events.pop(event_id)
    }

    /// Clones the event from pending events by id.
    fn clone_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        system.events.get(event_id).unwrap().clone()
    }

    /// Adds the event to pending events list.
    fn add_event(&self, system: &mut McSystem, event: McEvent) -> McEventId {
        system.events.push(event)
    }

    /// Applies corruption to the event data.
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

    /// Duplicates event from pending events list by id.
    /// The new event is left in pending events list and the old one is returned.
    fn duplicate_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        let event = self.take_event(system, event_id);
        system.events.push_with_fixed_id(event.duplicate().unwrap(), event_id);
        self.debug_log(&event, self.search_depth(), LogContext::Duplicated);
        event
    }

    /// Prints the log for particular event if the execution mode is [`Debug`](ExecutionMode::Debug).
    fn debug_log(&self, event: &McEvent, depth: u64, log_context: LogContext) {
        if self.execution_mode() == &ExecutionMode::Debug {
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

    /// Logs the message according to its log context.
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

    /// Determines the way of checking if the state was visited before.
    fn initialize_visited(log_mode: &ExecutionMode) -> VisitedStates
    where
        Self: Sized,
    {
        match log_mode {
            ExecutionMode::Debug => VisitedStates::Full(HashSet::default()),
            ExecutionMode::Default => VisitedStates::Partial(HashSet::default()),
        }
    }

    /// Checks if the system state was visited before.
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

    /// Marks the system state as already visited.
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

    /// Adds new information to model checking execution summary.
    fn update_summary(&mut self, status: String) {
        if let ExecutionMode::Debug = self.execution_mode() {
            let counter = self.summary().states.entry(status).or_insert(0);
            *counter += 1;
        }
    }

    /// Applies user-defined checking functions to the system state and gives the result of the check.
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

    /// Returns the execution mode of the model checking.
    fn execution_mode(&self) -> &ExecutionMode;

    /// Returns current search depth in system states graph.
    fn search_depth(&self) -> u64;

    /// Returns the visited states set.
    fn visited(&mut self) -> &mut VisitedStates;

    /// Returns the prune function.
    fn prune(&self) -> &PruneFn;

    /// Returns the goal function.
    fn goal(&self) -> &GoalFn;

    /// Returns th invariant function.
    fn invariant(&self) -> &InvariantFn;

    /// Returns the model checking execution summary.
    fn summary(&mut self) -> &mut McSummary;
}
