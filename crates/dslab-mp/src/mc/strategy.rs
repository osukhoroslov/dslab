//! Main logic and configuration of model checking strategy.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;
use sugars::boxed;

use crate::logger::LogEntry;
use crate::mc::error::McError;
use crate::mc::events::McEvent::{MessageCorrupted, MessageDropped, MessageDuplicated, MessageReceived, TimerCancelled, TimerFired};
use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::DeliveryOptions;
use crate::mc::state::McState;
use crate::mc::system::McSystem;
use crate::message::Message;
use crate::util::t;

/// Configuration of model checking strategy.
pub struct StrategyConfig {
    pub(crate) prune: PruneFn,
    pub(crate) goal: GoalFn,
    pub(crate) invariant: InvariantFn,
    pub(crate) collect: CollectFn,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) visited_states: VisitedStates,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            prune: boxed!(default_prune),
            goal: boxed!(default_goal),
            invariant: boxed!(default_invariant),
            collect: boxed!(default_collect),
            execution_mode: ExecutionMode::Default,
            visited_states: VisitedStates::Partial(HashSet::default()),
        }
    }
}

impl StrategyConfig {
    /// Sets prune function.
    pub fn prune(mut self, prune: PruneFn) -> Self {
        self.prune = prune;
        self
    }

    /// Sets invariant function.
    pub fn invariant(mut self, invariant: InvariantFn) -> Self {
        self.invariant = invariant;
        self
    }

    /// Sets goal function.
    pub fn goal(mut self, goal: GoalFn) -> Self {
        self.goal = goal;
        self
    }

    /// Sets collect function.
    pub fn collect(mut self, collect: CollectFn) -> Self {
        self.collect = collect;
        self
    }

    /// Sets execution mode.
    pub fn execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.execution_mode = execution_mode;
        self
    }

    /// Sets visited states cache.
    pub fn visited_states(mut self, visited_states: VisitedStates) -> Self {
        self.visited_states = visited_states;
        self
    }
}

pub(crate) fn default_prune(_: &McState) -> Option<String> {
    None
}

pub(crate) fn default_goal(_: &McState) -> Option<String> {
    None
}

pub(crate) fn default_invariant(_: &McState) -> Result<(), String> {
    Ok(())
}

pub(crate) fn default_collect(_: &McState) -> bool {
    false
}

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

    /// Does not store any data about the previously visited system states.
    Disabled,
}

/// Model checking execution statistics.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct McStats {
    /// Counters for statuses run achieved
    pub statuses: HashMap<String, u32>,
    /// States that were collected with Collect predicate
    pub collected_states: HashSet<McState>,
}

impl McStats {
    pub(crate) fn combine(&mut self, other: McStats) {
        self.collected_states.extend(other.collected_states.into_iter());
        for (state, cnt) in other.statuses {
            let entry = self.statuses.entry(state).or_insert(0);
            *entry += cnt;
        }
    }
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

/// Checks if given state should be collected.
/// Returns true if the state should be collected and false otherwise.
pub type CollectFn = Box<dyn FnMut(&McState) -> bool>;

/// Result of model checking run - statistics for successful run and error information for failure.
pub type McResult = Result<McStats, String>;

/// Trait with common functions for different model checking strategies.
pub trait Strategy {
    /// Builds a new strategy instance.
    fn build(config: StrategyConfig) -> Self
    where
        Self: Sized;

    /// Launches the strategy execution.
    fn run(&mut self, system: &mut McSystem) -> McResult;

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
            _ => {}
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
            event = self.corrupt_msg_data(system, event);
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

    /// Clones the event from pending events by id.
    fn clone_event(&self, system: &mut McSystem, event_id: McEventId) -> McEvent {
        system.events.get(event_id).unwrap().clone()
    }

    /// Adds the event to pending events list.
    fn add_event(&self, system: &mut McSystem, event: McEvent) -> McEventId {
        system.events.push(event)
    }

    /// Applies corruption to the MessageReceived event data.
    fn corrupt_msg_data(&self, system: &mut McSystem, event: McEvent) -> McEvent {
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
                let corrupted_msg = Message::new(msg.clone().tip, corrupted_data);
                let corruption_event = MessageCorrupted {
                    msg,
                    corrupted_msg: corrupted_msg.clone(),
                    src: src.clone(),
                    dest: dest.clone(),
                };
                system.apply_event(corruption_event.clone());
                self.debug_log(&corruption_event, system.depth());
                MessageReceived {
                    msg: corrupted_msg,
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
        let duplication_event = match event.clone() {
            MessageReceived { msg, src, dest, .. } => MessageDuplicated { msg, src, dest },
            _ => {
                panic!("Duplication is only allowed for messages")
            }
        };
        system.events.push_with_fixed_id(event.duplicate().unwrap(), event_id);
        system.apply_event(duplication_event.clone());
        self.debug_log(&duplication_event, system.depth());
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
                MessageDropped { msg, src, dest } => {
                    t!(format!(
                        "{:>10} | {:>10} --x {:<10} {:?} <-- message dropped",
                        depth, src, dest, msg
                    )
                    .red());
                }
                MessageDuplicated { msg, src, dest } => {
                    t!(format!(
                        "{:>10} | {:>10} -=≡ {:<10} {:?} <-- message duplicated",
                        depth, src, dest, msg
                    )
                    .blue());
                }
                MessageCorrupted { msg, corrupted_msg, src, dest } => {
                    t!(format!(
                        "{:>10} | {:>10} -x- {:<10} {:?} ~~> {:?} <-- message corrupted",
                        depth, src, dest, msg, corrupted_msg
                    )
                    .blue());
                }
            }
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
            VisitedStates::Disabled => false,
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
            VisitedStates::Disabled => {}
        }
    }

    /// Adds new information to model checking execution statistics.
    fn on_final_state_reached(&mut self, status: String) {
        if let ExecutionMode::Debug = self.execution_mode() {
            let counter = self.stats().statuses.entry(status).or_insert(0);
            *counter += 1;
        }
    }

    /// Saves trace which guides system to failure.
    fn update_failure_trace(&mut self, trace: Vec<LogEntry>) {
        *self.failure_trace() = trace;
    }

    /// Applies user-defined checking functions to the system state and returns the result of the check.
    fn check_state(&mut self, state: &McState) -> Option<Result<(), String>> {
        if (self.collect())(state) {
            self.stats().collected_states.insert(state.clone());
        }
        if let Err(err) = (self.invariant())(state) {
            // Invariant is broken
            self.update_failure_trace(state.trace.clone());
            Some(Err(err))
        } else if let Some(status) = (self.goal())(state) {
            // Reached final state of the system
            self.on_final_state_reached(status);
            Some(Ok(()))
        } else if let Some(status) = (self.prune())(state) {
            // Execution branch is pruned
            self.on_final_state_reached(status);
            Some(Ok(()))
        } else if state.events.available_events_num() == 0 {
            // exhausted without goal completed
            Some(Err("nothing left to do to reach the goal".to_owned()))
        } else {
            None
        }
    }

    /// Set collect predicate.
    fn set_collect(&mut self, collect: CollectFn) {
        *self.collect() = collect;
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

    /// Returns the collect function.
    fn collect(&mut self) -> &mut CollectFn;

    /// Returns the model checking execution stats.
    fn stats(&mut self) -> &mut McStats;

    /// Returns the failure trace in state graph.
    fn failure_trace(&mut self) -> &mut Vec<LogEntry>;
}
