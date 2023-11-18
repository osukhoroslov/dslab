//! Standard predicate implementations that can be used in model checking strategy.

use crate::{logger::LogEntry, mc::state::McState};

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

pub(crate) fn count_events_in_trace<F>(predicate: F, state: &McState) -> usize
where
    F: Fn(&LogEntry) -> bool,
{
    state.trace.iter().filter(|x| predicate(x)).count()
}

/// Invariants check whether state is correct or not.
pub mod invariants {
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    use sugars::boxed;

    use crate::mc::state::McState;
    use crate::mc::strategy::InvariantFn;

    /// Combines multiple invariant functions by returning `Ok` iff all invariants are satisfied.
    pub fn all_invariants(mut rules: Vec<InvariantFn>) -> InvariantFn {
        boxed!(move |state: &McState| {
            for rule in &mut rules {
                rule(state)?;
            }
            Ok(())
        })
    }

    /// Checks that state depth does not exceed the given value.
    pub fn state_depth(depth: u64) -> InvariantFn {
        boxed!(move |state: &McState| {
            if state.depth > depth {
                Err(format!("state depth exceeds maximum allowed depth {depth}"))
            } else {
                Ok(())
            }
        })
    }

    /// Checks that state depth for current run does not exceed the given value.
    pub fn state_depth_current_run(depth: u64) -> InvariantFn {
        boxed!(move |state: &McState| {
            if state.current_run_trace().len() > depth as usize {
                Err(format!("state depth exceeds maximum allowed depth {depth}"))
            } else {
                Ok(())
            }
        })
    }

    /// Checks that overall run duration does not exceed the given time limit.
    pub fn time_limit(time_limit: Duration) -> InvariantFn {
        let start_time = Instant::now();
        // We use counter to calculate time 1 out of 256 calls for performance purposes.
        let mut counter: u8 = 0;
        boxed!(move |_: &McState| {
            if counter == 0 && start_time.elapsed() > time_limit {
                return Err(format!("time limit of {}s exceeded", time_limit.as_secs_f32()));
            }
            counter = counter.wrapping_add(1);
            Ok(())
        })
    }

    /// Verifies that the set of local messages delivered by a process matches exactly the expected messages.
    /// Message duplications or unexpected messages are not allowed.
    pub fn received_messages<S>(node: S, proc: S, messages_expected: HashSet<String>) -> InvariantFn
    where
        S: Into<String>,
    {
        let node = node.into();
        let proc = proc.into();
        boxed!(move |state: &McState| {
            let local_outbox = &state.node_states[&node].proc_states[&proc].local_outbox;
            let mut messages_got = HashSet::<String>::default();
            if local_outbox.len() > messages_expected.len() {
                return Err(format!(
                    "{proc} received at least {} messages but only {} expected",
                    local_outbox.len(),
                    messages_expected.len()
                ));
            }
            if local_outbox.len() < messages_expected.len() && state.events.is_empty() {
                return Err(format!(
                    "{proc} received {} messages in total but {} expected",
                    local_outbox.len(),
                    messages_expected.len()
                ));
            }
            for message in local_outbox {
                if !messages_got.insert(message.data.clone()) {
                    return Err(format!("message {:?} was duplicated", message));
                }
                if !messages_expected.contains(&message.data) {
                    return Err(format!("message {:?} is not expected", message));
                }
            }
            Ok(())
        })
    }
}

/// Goals check if state is final.
pub mod goals {
    use sugars::boxed;

    use crate::logger::LogEntry;
    use crate::mc::state::McState;
    use crate::mc::strategy::GoalFn;

    /// Combines multiple goal functions by returning `Some()` iff at least one goal is reached.
    pub fn any_goal(mut goals: Vec<GoalFn>) -> GoalFn {
        boxed!(move |state: &McState| {
            for goal in &mut goals {
                if let Some(status) = goal(state) {
                    return Some(status);
                }
            }
            None
        })
    }

    /// Combines multiple goal functions by returning `Some()` iff all goals are reached.
    pub fn all_goals(mut goals: Vec<GoalFn>) -> GoalFn {
        boxed!(move |state: &McState| {
            for goal in &mut goals {
                goal(state)?;
            }
            Some("combined goal is reached".to_string())
        })
    }

    /// Checks if the given process produced `n` local messages.
    pub fn got_n_local_messages<S>(node: S, proc: S, n: usize) -> GoalFn
    where
        S: Into<String>,
    {
        let node = node.into();
        let proc = proc.into();
        boxed!(move |state: &McState| {
            if state.node_states[&node].proc_states[&proc].local_outbox.len() == n {
                Some(format!("{proc} produced {n} local messages"))
            } else {
                None
            }
        })
    }

    /// Checks if current state has no more active events.
    pub fn no_events() -> GoalFn {
        boxed!(|state: &McState| {
            if state.events.is_empty() {
                Some("final state reached".to_string())
            } else {
                None
            }
        })
    }

    /// Checks if current state is on given depth.
    pub fn depth_reached(depth: u64) -> GoalFn {
        boxed!(move |state: &McState| {
            if state.depth >= depth {
                Some("final depth reached".to_string())
            } else {
                None
            }
        })
    }

    /// This goal is used when start state is also a final state.
    pub fn always_ok() -> GoalFn {
        boxed!(move |_: &McState| { Some("ok".to_string()) })
    }

    /// Checks if current run trace has at least `n` events matching the predicate.
    pub fn event_happened_n_times_current_run<F>(predicate: F, n: usize) -> GoalFn
    where
        F: Fn(&LogEntry) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            let event_count = state.current_run_trace().iter().filter(|x| predicate(x)).count();
            if event_count >= n {
                Some(format!("event occured {event_count} >= {n} times"))
            } else {
                None
            }
        })
    }
}

/// Prunes cut execution branches if further analysis is considered unnecessary or computation-heavy.
pub mod prunes {
    use std::collections::HashSet;

    use sugars::boxed;

    use crate::logger::LogEntry;
    use crate::mc::predicates::count_events_in_trace;
    use crate::mc::state::McState;
    use crate::mc::strategy::PruneFn;

    /// Combines multiple prune functions by returning `Some()` iff at least one prune is satisfied.
    pub fn any_prune(mut prunes: Vec<PruneFn>) -> PruneFn {
        boxed!(move |state: &McState| {
            for prune in &mut prunes {
                if let Some(status) = prune(state) {
                    return Some(status);
                }
            }
            None
        })
    }

    /// Prunes states with depth exceeding the given value.
    pub fn state_depth(depth: u64) -> PruneFn {
        boxed!(move |state: &McState| {
            if state.depth > depth {
                Some(format!(
                    "state depth exceeds maximum depth {depth} that is under consideration"
                ))
            } else {
                None
            }
        })
    }

    /// Prunes states where some process has sent more messages than the given value.
    pub fn sent_messages_limit(max_allowed_messages: u64) -> PruneFn {
        boxed!(move |state: &McState| {
            for (_, node) in state.node_states.iter() {
                for (proc_name, proc) in node.proc_states.iter() {
                    if proc.sent_message_count > max_allowed_messages {
                        return Some(format!("too many messages sent by {proc_name}"));
                    }
                }
            }
            None
        })
    }

    /// Prunes state with at least `n` events matching the predicate.
    pub fn event_happened_n_times_current_run<F>(predicate: F, n: usize) -> PruneFn
    where
        F: Fn(&LogEntry) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            let event_count = state.current_run_trace().iter().filter(|x| predicate(x)).count();
            if event_count >= n {
                Some(format!("event occured {n} times"))
            } else {
                None
            }
        })
    }

    /// Prunes states where the number of events matching the predicate is more than the limit.
    pub fn events_limit<F>(predicate: F, limit: usize) -> PruneFn
    where
        F: Fn(&LogEntry) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            let event_count = count_events_in_trace(&predicate, state);
            if event_count > limit {
                Some(format!(
                    "event occured {event_count} times but expected at most {limit} times"
                ))
            } else {
                None
            }
        })
    }

    /// Prunes states where the number of events matching the predicate is more than the limit for any of processes.
    pub fn events_limit_per_proc<F>(predicate: F, process_names: Vec<String>, limit: usize) -> PruneFn
    where
        F: Fn(&LogEntry, &String) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            for proc in &process_names {
                let proc_predicate = |entry: &LogEntry| predicate(entry, proc);
                let event_count = count_events_in_trace(proc_predicate, state);
                if event_count > limit {
                    return Some(format!(
                        "event occured {event_count} times on proc {proc} but expected at most {limit} times"
                    ));
                }
            }
            None
        })
    }

    /// Prunes states where processes are mentioned in any permutation except the given one.
    /// It helps to create symmetry-breaking constraints
    pub fn proc_permutations(equivalent_procs: &[String]) -> PruneFn {
        let equivalent_procs = equivalent_procs.to_vec();
        boxed!(move |state| {
            let proc_names = HashSet::<String>::from_iter(equivalent_procs.clone().into_iter());
            let mut used_proc_names = HashSet::<String>::new();
            let mut waiting_for_proc = 0;
            for entry in state.current_run_trace() {
                match entry {
                    LogEntry::McMessageReceived { src: proc, .. } | LogEntry::McTimerFired { proc, .. } => {
                        if used_proc_names.contains(proc) || !proc_names.contains(proc) {
                            continue;
                        }
                        if equivalent_procs[waiting_for_proc] != *proc {
                            return Some("state is the same as another state with renumerated processes".to_owned());
                        }
                        used_proc_names.insert(proc.clone());
                        waiting_for_proc += 1;
                    }
                    _ => {}
                }
            }
            None
        })
    }
}

/// Collects select states that should be collected for complex pipelining in MC.
pub mod collects {
    use sugars::boxed;

    use crate::logger::LogEntry;
    use crate::mc::predicates::count_events_in_trace;
    use crate::mc::state::McState;
    use crate::mc::strategy::CollectFn;

    /// Checks if the given process produced `n` local messages.
    pub fn got_n_local_messages<S>(node: S, proc: S, n: usize) -> CollectFn
    where
        S: Into<String>,
    {
        let node = node.into();
        let proc = proc.into();
        boxed!(move |state: &McState| state.node_states[&node].proc_states[&proc].local_outbox.len() == n)
    }

    /// Combines multiple collect functions by returning `true` iff at least one collect is satisfied.
    pub fn any_collect(mut collects: Vec<CollectFn>) -> CollectFn {
        boxed!(move |state: &McState| {
            for collect in &mut collects {
                if collect(state) {
                    return true;
                }
            }
            false
        })
    }

    /// Combines multiple collect functions by returning `true` iff all collects are satisfied.
    pub fn all_collects(mut collects: Vec<CollectFn>) -> CollectFn {
        boxed!(move |state: &McState| {
            for collect in &mut collects {
                if !collect(state) {
                    return false;
                }
            }
            true
        })
    }

    /// Checks if trace to given state has at least `n` events matching the predicate.
    pub fn event_happened_n_times_current_run<F>(predicate: F, n: usize) -> CollectFn
    where
        F: Fn(&LogEntry) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            let event_count = state.current_run_trace().iter().filter(|x| predicate(x)).count();
            event_count >= n
        })
    }

    /// Checks if current state has no more active events.
    pub fn no_events() -> CollectFn {
        boxed!(|state: &McState| { state.events.is_empty() })
    }

    /// Checks if current state's depth exceeds the given value.
    pub fn state_depth(depth: u64) -> CollectFn {
        boxed!(move |state: &McState| { state.depth > depth })
    }

    /// Collects states where the number of events matching the predicate is more than the limit.
    pub fn events_limit<F>(predicate: F, limit: usize) -> CollectFn
    where
        F: Fn(&LogEntry) -> bool + 'static,
    {
        boxed!(move |state: &McState| {
            let event_count = count_events_in_trace(&predicate, state);
            event_count > limit
        })
    }
}
