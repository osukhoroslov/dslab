//! Main logic and configuration of model checking strategy.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;

use crate::mc::events::McEvent::{MessageDropped, MessageReceived, TimerCancelled, TimerFired};
use crate::mc::events::{DeliveryOptions, McEvent, McEventId};
use crate::mc::state::McState;
use crate::mc::system::McSystem;
use crate::message::Message;
use crate::util::t;

/// Defines the mode in which the model checking algorithm is executing.
#[derive(Clone, PartialEq)]
pub enum ExecutionMode {
    /// Default execution mode with reduced output intended for regular use.
    Default,

    /// Execution with verbose output intended for debugging purposes.
    /// Runs slower than the default mode.
    Debug,
}

/// Status of the message which is passed to logging.
pub enum LogContext {
    /// Nothing happened to the message.
    Default,

    /// Message was duplicated.
    Duplicated,

    /// Message data was corrupted.
    Corrupted,
}

/// Alternative implementations of storing the previously visited system states
/// and checking if the state was visited.
pub enum VisitedStates {
    /// Stores the visited states and checks for equality of states (slower, requires more memory).
    Full(HashSet<McState>),

    /// Stores the hashes of visited states and checks for equality of state hashes
    /// (faster, requires less memory, but may have false positives due to collisions).
    Partial(HashSet<u64>),
}

/// Model checking execution summary (used in Debug mode).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct McSummary {
    pub(crate) states: HashMap<String, u32>,
}

/// Decides whether to prune the executions originating from the given state.
/// Returns Some(status) if the executions should be pruned and None otherwise.
pub type PruneFn = Box<dyn FnMut(&McState) -> Option<String>>;

/// Checks if the given state is the final state, i.e. all expected events have already occurred.
/// Returns Some(status) if the final state is reached and None otherwise.
pub type GoalFn = Box<dyn FnMut(&McState) -> Option<String>>;

/// Checks if some invariant holds in the given state.
/// Returns Err(error) if the invariant is broken and Ok otherwise.
pub type InvariantFn = Box<dyn FnMut(&McState) -> Result<(), String>>;

/// Trait with common functions for different model checking strategies.
pub trait Strategy {
    /// Launches the strategy execution.
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String>;

    /// Callback which in called whenever a new system state is discovered.
    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), String>;

    /// Explores the possible system states reachable from the current state after processing the given event.
    /// Calls `search_step_impl` for each new discovered state.    
    fn process_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), String> {
        let event = self.clone_event(system, event_id);
        match event {
            MessageReceived {
                msg,
                src,
                dest,
                options,
            } => {
                match options {
                    DeliveryOptions::NoFailures(..) => self.apply_event(system, event_id, false, false)?,
                    DeliveryOptions::Dropped => self.process_drop_event(system, event_id, msg, src, dest)?,
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    } => {
                        // Drop
                        if can_be_dropped {
                            self.process_drop_event(system, event_id, msg, src, dest)?;
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
            TimerCancelled { proc, timer } => {
                system.events.cancel_timer(proc, timer);
                self.apply_event(system, event_id, false, false)?;
            }
            MessageDropped { .. } => {
                self.apply_event(system, event_id, false, false)?;
            }
        }

        Ok(())
    }

    /// Processes event dropping.
    fn process_drop_event(
        &mut self,
        system: &mut McSystem,
        event_id: McEventId,
        msg: Message,
        src: String,
        dest: String,
    ) -> Result<(), String> {
        let state = system.get_state();
        self.take_event(system, event_id);

        let drop_event_id = self.add_event(system, MessageDropped { msg, src, dest });

        self.apply_event(system, drop_event_id, false, false)?;
        system.set_state(state);

        Ok(())
    }

    /// Applies (possibly modified) event to the system, calls `search_step_impl` with the produced state
    /// and restores the system state afterwards.
    fn apply_event(
        &mut self,
        system: &mut McSystem,
        event_id: McEventId,
        duplicate: bool,
        corrupt: bool,
    ) -> Result<(), String> {
        let state = system.get_state();

        let mut event;
        if duplicate {
            event = self.duplicate_event(system, event_id);
        } else {
            event = self.take_event(system, event_id);
        }

        if corrupt {
            event = self.corrupt_msg_data(event, system.depth());
        }

        self.debug_log(&event, LogContext::Default, system.depth());

        system.apply_event(event);

        let new_state = system.get_state();
        if !self.have_visited(&new_state) {
            self.mark_visited(system.get_state());
            self.search_step_impl(system, new_state)?;
        }

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

    /// Applies corruption to the MessageReceived event data.
    fn corrupt_msg_data(&self, event: McEvent, search_depth: u64) -> McEvent {
        self.debug_log(&event, LogContext::Corrupted, search_depth);
        match event {
            MessageReceived {
                msg,
                src,
                dest,
                options,
            } => {
                lazy_static! {
                    static ref RE: Regex = Regex::new(r#""[^"]+""#).unwrap();
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
        self.debug_log(&event, LogContext::Duplicated, system.depth());
        event
    }

    /// Prints the log for particular event if the execution mode is [`Debug`](ExecutionMode::Debug).
    fn debug_log(&self, event: &McEvent, log_context: LogContext, depth: u64) {
        if self.execution_mode() == &ExecutionMode::Debug {
            match event {
                MessageReceived { msg, src, dest, .. } => {
                    self.log_message(depth, msg, src, dest, log_context);
                }
                TimerFired { proc, timer, .. } => {
                    t!(format!("{:>10} | {:>10} !-- {:<10} <-- timer fired", depth, proc, timer).yellow());
                }
                TimerCancelled { proc, timer } => {
                    t!(format!("{:>10} | {:>10} xxx {:<10} <-- timer cancelled", depth, proc, timer).yellow());
                }
                MessageDropped { msg, src, dest, .. } => {
                    t!(format!(
                        "{:>10} | {:>10} --x {:<10} {:?} <-- message dropped",
                        depth, src, dest, msg
                    )
                    .red());
                }
            }
        }
    }

    /// Logs the message according to its log context.
    fn log_message(&self, depth: u64, msg: &Message, src: &String, dest: &String, log_context: LogContext) {
        match log_context {
            LogContext::Default => {
                t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dest, src, msg);
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

    /// Applies user-defined checking functions to the system state and returns the result of the check.
    fn check_state(&mut self, state: &McState) -> Option<Result<(), String>> {
        if let Err(err) = (self.invariant())(state) {
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
        } else if state.events.available_events_num() == 0 {
            // exhausted without goal completed
            Some(Err("nothing left to do to reach the goal".to_owned()))
        } else {
            None
        }
    }

    /// Returns the used execution mode.
    fn execution_mode(&self) -> &ExecutionMode;

    /// Returns the visited states set.
    fn visited(&mut self) -> &mut VisitedStates;

    /// Returns the prune function.
    fn prune(&mut self) -> &mut PruneFn;

    /// Returns the goal function.
    fn goal(&mut self) -> &mut GoalFn;

    /// Returns the invariant function.
    fn invariant(&mut self) -> &mut InvariantFn;

    /// Returns the model checking execution summary.
    fn summary(&mut self) -> &mut McSummary;
}
