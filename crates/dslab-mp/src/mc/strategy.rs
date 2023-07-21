//! Main logic and configuration of model checking strategy.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;

use crate::mc::error::McError;
use crate::mc::events::McEvent::{
    MessageCorrupted, MessageDropped, MessageDuplicated, MessageReceived, TimerCancelled, TimerFired,
};
use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::DeliveryOptions;
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

/// Alternative implementations of storing the previously visited system states
/// and checking if the state was visited.
pub enum VisitedStates {
    /// Stores the visited states and checks for equality of states (slower, requires more memory).
    Full(HashSet<McState>),

    /// Stores the hashes of visited states and checks for equality of state hashes
    /// (faster, requires less memory, but may have false positives due to collisions).
    Partial(HashSet<u64>),
}

/// Holds either [`McEvent`] or [`McEventId`].
#[allow(missing_docs)]
pub enum EventOrId {
    Event(McEvent),
    Id(McEventId),
}

/// Model checking execution summary (used in Debug mode).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct McSummary {
    pub(crate) statuses: HashMap<String, u32>,
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
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, McError>;

    /// Callback which in called whenever a new system state is discovered.
    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), McError>;

    /// Explores the possible system states reachable from the current state after processing the given event.
    /// Calls `search_step_impl` for each new discovered state.
    fn process_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), McError> {
        let event = system.events.get(event_id).unwrap();
        match event {
            MessageReceived {
                msg,
                src,
                dest,
                options,
            } => {
                match *options {
                    DeliveryOptions::NoFailures(..) => self.search_step(system, EventOrId::Id(event_id))?,
                    DeliveryOptions::PossibleFailures {
                        can_be_dropped,
                        max_dupl_count,
                        can_be_corrupted,
                    } => {
                        // Clone needed data here to avoid cloning normal events without failures
                        let msg = msg.clone();
                        let src = src.clone();
                        let dest = dest.clone();

                        // Message drop (prefer to explore it before the normal delivery)
                        if can_be_dropped {
                            let drop_event = MessageDropped {
                                msg: msg.clone(),
                                src: src.clone(),
                                dest: dest.clone(),
                                receive_event_id: Some(event_id),
                            };
                            self.search_step(system, EventOrId::Event(drop_event))?;
                        }

                        // Normal delivery
                        self.search_step(system, EventOrId::Id(event_id))?;

                        // Message corruption
                        if can_be_corrupted {
                            let corruption_event = MessageCorrupted {
                                msg: msg.clone(),
                                corrupted_msg: msg.clone(),
                                src: src.clone(),
                                dest: dest.clone(),
                                receive_event_id: event_id,
                            };
                            self.search_step(system, EventOrId::Event(corruption_event))?;
                        }

                        // Message duplication
                        if max_dupl_count > 0 {
                            let duplication_event = MessageDuplicated {
                                msg: msg.clone(),
                                src: src.clone(),
                                dest: dest.clone(),
                                receive_event_id: event_id,
                            };
                            self.search_step(system, EventOrId::Event(duplication_event))?;
                        }
                    }
                }
            }
            TimerCancelled { proc, timer } => {
                system.events.cancel_timer(proc.clone(), timer.clone());
                self.search_step(system, EventOrId::Id(event_id))?
            }
            _ => self.search_step(system, EventOrId::Id(event_id))?,
        }

        Ok(())
    }

    /// Makes search step according to `step_type`, calls `search_step_impl` with the produced state
    /// and restores the system state afterwards.
    fn search_step(&mut self, system: &mut McSystem, event: EventOrId) -> Result<(), McError> {
        let state = system.get_state();

        let mut event = match event {
            EventOrId::Event(event) => event,
            EventOrId::Id(event_id) => self.take_event(system, event_id),
        };

        match &mut event {
            MessageDropped { receive_event_id, .. } => {
                receive_event_id.map(|id| self.take_event(system, id));
            }
            MessageDuplicated { receive_event_id, .. } => {
                let dupl_event = self.duplicate_event(system, *receive_event_id);
                self.add_event(system, dupl_event);
            }
            MessageCorrupted {
                receive_event_id,
                corrupted_msg,
                ..
            } => {
                let msg = self.take_event(system, *receive_event_id);
                let corrupted = self.corrupt_msg_data(msg, corrupted_msg);
                system.events.push_with_fixed_id(corrupted, *receive_event_id);
            }
            _ => {}
        }

        self.debug_log(&event, system.depth());

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

    /// Adds the event to pending events list.
    fn add_event(&self, system: &mut McSystem, event: McEvent) -> McEventId {
        system.events.push(event)
    }

    /// Applies corruption to the MessageReceived event data.
    fn corrupt_msg_data(&self, mut event: McEvent, corrupted_msg: &mut Message) -> McEvent {
        match &mut event {
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
                *corrupted_msg = Message::new(msg.clone().tip, corrupted_data);

                if let DeliveryOptions::PossibleFailures { can_be_corrupted, .. } = options {
                    *can_be_corrupted = false;
                }

                MessageReceived {
                    msg: corrupted_msg.clone(),
                    src: src.clone(),
                    dest: dest.clone(),
                    options: options.clone(),
                }
            }
            _ => event,
        }
    }

    /// Duplicates event from pending events list by id.
    /// The new event is left in pending events list and the old one is returned.
    fn duplicate_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        let mut event = self.take_event(system, event_id);

        system.events.push_with_fixed_id(event.duplicate().unwrap(), event_id);

        event.disable_duplications();
        event
    }

    /// Prints the log for particular event if the execution mode is [`Debug`](ExecutionMode::Debug).
    fn debug_log(&self, event: &McEvent, depth: u64) {
        if self.execution_mode() == &ExecutionMode::Debug {
            match event {
                MessageReceived { msg, src, dest, .. } => {
                    t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dest, src, msg);
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
                MessageDuplicated { msg, src, dest, .. } => {
                    t!(format!(
                        "{:>10} | {:>10} -=≡ {:<10} {:?} <-- message duplicated",
                        depth, src, dest, msg
                    )
                    .blue());
                }
                MessageCorrupted {
                    msg,
                    corrupted_msg,
                    src,
                    dest,
                    ..
                } => {
                    t!(format!(
                        "{:>10} | {:>10} -x- {:<10} {:?} ~~> {:?} <-- message corrupted",
                        depth, src, dest, msg, corrupted_msg
                    )
                    .blue());
                }
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

    /// Adds new status to model checking execution summary.
    fn update_summary_statuses(&mut self, status: String) {
        if let ExecutionMode::Debug = self.execution_mode() {
            let counter = self.summary().statuses.entry(status).or_insert(0);
            *counter += 1;
        }
    }

    /// Applies user-defined checking functions to the system state and returns the result of the check.
    fn check_state(&mut self, state: &McState) -> Option<Result<(), McError>> {
        if let Err(err) = (self.invariant())(state) {
            // Invariant is broken
            Some(Err(McError::new(err, state.trace.clone())))
        } else if let Some(status) = (self.goal())(state) {
            // Reached final state of the system
            self.update_summary_statuses(status);
            Some(Ok(()))
        } else if let Some(status) = (self.prune())(state) {
            // Execution branch is pruned
            self.update_summary_statuses(status);
            Some(Ok(()))
        } else if state.events.available_events_num() == 0 {
            // exhausted without goal completed
            Some(Err(McError::new(
                "nothing left to do to reach the goal".to_owned(),
                state.trace.clone(),
            )))
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
