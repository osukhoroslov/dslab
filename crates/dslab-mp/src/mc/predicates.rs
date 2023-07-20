//! Standard predicate implementations that can be used in model checking strategy.

use std::collections::HashSet;

use sugars::boxed;

use crate::mc::state::McState;
use crate::mc::strategy::GoalFn;
use crate::mc::strategy::InvariantFn;
use crate::mc::strategy::PruneFn;

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

/// Combines multiple invariant functions by returning `Ok` iff all invariants are satisfied.
pub fn mc_all_invariants(mut rules: Vec<InvariantFn>) -> InvariantFn {
    boxed!(move |state: &McState| {
        for rule in &mut rules {
            rule(state)?;
        }
        Ok(())
    })
}

/// Combines multiple goal functions by returning `Ok` iff at least one goal is reached.
pub fn mc_any_goal(mut goals: Vec<GoalFn>) -> GoalFn {
    boxed!(move |state: &McState| {
        for goal in &mut goals {
            if let Some(status) = goal(state) {
                return Some(status);
            }
        }
        None
    })
}

/// Checks if the given process produced `n` local messages.
pub fn mc_goal_got_n_local_messages(node: String, proc: String, n: u64) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.node_states[&node][&proc].local_outbox.len() == n as usize {
            Some(format!("{proc} produced {n} local messages"))
        } else {
            None
        }
    })
}

/// Checks if current state has no more active events.
pub fn mc_goal_nothing_left_to_do() -> GoalFn {
    boxed!(|state: &McState| {
        if state.events.available_events_num() == 0 {
            Some("final state reached".to_string())
        } else {
            None
        }
    })
}

/// Checks that state depth does not exceed the given value.
pub fn mc_invariant_state_depth(depth: u64) -> InvariantFn {
    boxed!(move |state: &McState| {
        if state.depth > depth {
            Err(format!("state depth exceeds maximum allowed depth {depth}"))
        } else {
            Ok(())
        }
    })
}

/// Prunes states with depth exceeding the given value.
pub fn mc_prune_state_depth(depth: u64) -> PruneFn {
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
pub fn mc_prune_sent_messages_limit(max_allowed_messages: u64) -> PruneFn {
    boxed!(move |state: &McState| {
        for (_, node) in state.node_states.iter() {
            for (proc_name, proc) in node.iter() {
                if proc.sent_message_count > max_allowed_messages {
                    return Some(format!("too many messages sent by {proc_name}"));
                }
            }
        }
        None
    })
}

/// Verifies that the set of local messages delivered by a process matches exactly the expected messages.
/// Message duplications or unexpected messages are not allowed.
pub fn mc_invariant_received_messages(node: String, proc: String, messages_expected: HashSet<String>) -> InvariantFn {
    boxed!(move |state: &McState| {
        if state.events.available_events_num() > 0 {
            return Ok(());
        }
        let mut messages_got = HashSet::<String>::default();
        let local_outbox = &state.node_states[&node][&proc].local_outbox;
        if local_outbox.len() != messages_expected.len() {
            return Err(format!(
                "{proc} received {} messages but {} expected",
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
