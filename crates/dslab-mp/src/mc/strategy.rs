//! Main logic and configuration of model checking strategy.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use colored::*;
use lazy_static::lazy_static;
use regex::Regex;
use sugars::boxed;

use crate::mc::error::McError;
use crate::mc::events::McEvent::{
    MessageCorrupted, MessageDropped, MessageDuplicated, MessageReceived, TimerCancelled, TimerFired,
};
use crate::mc::events::{McEvent, McEventId};
use crate::mc::network::DeliveryOptions;
use crate::mc::predicates;
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
            prune: boxed!(predicates::default_prune),
            goal: boxed!(predicates::default_goal),
            invariant: boxed!(predicates::default_invariant),
            collect: boxed!(predicates::default_collect),
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

/// Holds either [`McEvent`] or [`McEventId`].
#[allow(missing_docs)]
pub enum EventOrId {
    Event(McEvent),
    Id(McEventId),
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
        self.collected_states.extend(other.collected_states);
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
pub type McResult = Result<McStats, McError>;

/// Trait with common functions for different model checking strategies.
pub trait Strategy {
    /// Builds a new strategy instance.
    fn build(config: StrategyConfig) -> Self
    where
        Self: Sized;

    /// Launches the strategy execution.
    fn run(&mut self, system: &mut McSystem) -> McResult;

    /// Callback which in called whenever a new system state is discovered.
    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), McError>;

    /// Explores the possible system states reachable from the current state after processing the given event.
    /// Calls `search_step_impl` for each new discovered state.
    fn process_event(&mut self, system: &mut McSystem, event_id: McEventId) -> Result<(), McError> {
        let event = system.events.get(event_id).unwrap();
        match event {
            MessageReceived { msg, src, dst, options } => {
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
                        let dst = dst.clone();

                        // Normal delivery
                        self.search_step(system, EventOrId::Id(event_id))?;

                        // Message drop
                        if can_be_dropped {
                            let drop_event = MessageDropped {
                                msg: msg.clone(),
                                src: src.clone(),
                                dst: dst.clone(),
                                receive_event_id: Some(event_id),
                            };
                            self.search_step(system, EventOrId::Event(drop_event))?;
                        }

                        // Message corruption
                        if can_be_corrupted {
                            let corruption_event = MessageCorrupted {
                                msg: msg.clone(),
                                corrupted_msg: self.corrupt_message(msg.clone()),
                                src: src.clone(),
                                dst: dst.clone(),
                                receive_event_id: event_id,
                            };
                            self.search_step(system, EventOrId::Event(corruption_event))?;
                        }

                        // Message duplication
                        if max_dupl_count > 0 {
                            let duplication_event = MessageDuplicated {
                                msg,
                                src,
                                dst,
                                receive_event_id: event_id,
                            };
                            self.search_step(system, EventOrId::Event(duplication_event))?;
                        }
                    }
                }
            }
            _ => self.search_step(system, EventOrId::Id(event_id))?,
        }

        Ok(())
    }

    /// Applies the specified event to the system, calls `search_step_impl` with the produced state
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
                let original_event = self.take_event(system, *receive_event_id);
                let corrupted = self.create_corrupted_receive(original_event, corrupted_msg.clone());
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

    /// Applies corruption to the Message.
    fn corrupt_message(&self, mut msg: Message) -> Message {
        lazy_static! {
            static ref RE: Regex = Regex::new(r#""[^"]+""#).unwrap();
        }
        msg.data = RE.replace_all(&msg.data, "\"\"").to_string();
        msg
    }

    /// Creates MessageReceived event with corrupted msg.
    fn create_corrupted_receive(&self, event: McEvent, corrupted_msg: Message) -> McEvent {
        if let MessageReceived {
            src, dst, mut options, ..
        } = event
        {
            if let DeliveryOptions::PossibleFailures { can_be_corrupted, .. } = &mut options {
                *can_be_corrupted = false;
            }
            MessageReceived {
                msg: corrupted_msg,
                src,
                dst,
                options,
            }
        } else {
            panic!("Unexpected event type")
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
                MessageReceived { msg, src, dst, .. } => {
                    t!("{:>10} | {:>10} <-- {:<10} {:?}", depth, dst, src, msg);
                }
                TimerFired { proc, timer, .. } => {
                    t!(format!("{:>10} | {:>10} !-- {:<10} <-- timer fired", depth, proc, timer).yellow());
                }
                TimerCancelled { proc, timer } => {
                    t!(format!("{:>10} | {:>10} xxx {:<10} <-- timer cancelled", depth, proc, timer).yellow());
                }
                MessageDropped { msg, src, dst, .. } => {
                    t!(format!(
                        "{:>10} | {:>10} --x {:<10} {:?} <-- message dropped",
                        depth, src, dst, msg
                    )
                    .red());
                }
                MessageDuplicated { msg, src, dst, .. } => {
                    t!(format!(
                        "{:>10} | {:>10} -=≡ {:<10} {:?} <-- message duplicated",
                        depth, src, dst, msg
                    )
                    .blue());
                }
                MessageCorrupted {
                    msg,
                    corrupted_msg,
                    src,
                    dst,
                    ..
                } => {
                    t!(format!(
                        "{:>10} | {:>10} -x- {:<10} {:?} ~~> {:?} <-- message corrupted",
                        depth, src, dst, msg, corrupted_msg
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

    /// Applies user-defined checking functions to the system state and returns the result of the check.
    fn check_state(&mut self, state: &McState) -> Option<Result<(), McError>> {
        if (self.collect())(state) {
            self.stats().collected_states.insert(state.clone());
        }
        if let Err(err) = (self.invariant())(state) {
            // Invariant is broken
            Some(Err(McError::new(err, state.trace.clone())))
        } else if let Some(status) = (self.goal())(state) {
            // Reached final state of the system
            self.on_final_state_reached(status);
            Some(Ok(()))
        } else if let Some(status) = (self.prune())(state) {
            // Execution branch is pruned
            self.on_final_state_reached(status);
            Some(Ok(()))
        } else if state.events.is_empty() {
            // exhausted without goal completed
            Some(Err(McError::new(
                "nothing left to do to reach the goal".to_owned(),
                state.trace.clone(),
            )))
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

    /// Resets the internal state so that the strategy can be safely run again.
    fn reset(&mut self);

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
}
