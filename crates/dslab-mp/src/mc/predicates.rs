//! Predicates that can be used for configuration in model checker

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

/// Chains invariants using AND predicate.
pub fn mc_invariant_combined(mut rules: Vec<InvariantFn>) -> InvariantFn {
    boxed!(move |state| {
        for rule in &mut rules {
            rule(state)?;
        }
        Ok(())
    })
}

/// Chains goals using OR predicate.
pub fn mc_goal_combined(mut goals: Vec<GoalFn>) -> GoalFn {
    boxed!(move |state| {
        for goal in &mut goals {
            if let Some(status) = goal(state) {
                return Some(status);
            }
        }
        None
    })
}

/// Checks if n messages were received by a given process.
pub fn mc_goal_got_n_local_messages(node: String, proc: String, n: u64) -> GoalFn {
    boxed!(move |state| {
        if state.node_states[&node][&proc].local_outbox.len() == n as usize {
            Some(format!("{proc} produced {n} local messages"))
        } else {
            None
        }
    })
}

/// Checks if current state has no more active events.
pub fn mc_goal_nothing_left_to_do() -> GoalFn {
    boxed!(|state| {
        if state.events.available_events_num() == 0 {
            Some("final state reached".to_string())
        } else {
            None
        }
    })
}

/// Checks if state is located deeper that allowed
pub fn mc_invariant_state_depth(depth: u64) -> InvariantFn {
    boxed!(move |state| {
        if state.depth > depth {
            Err(format!("state depth exceeds maximum allowed depth {depth}"))
        } else {
            Ok(())
        }
    })
}

/// Checks if state is located deep enough to skip it during model checking run
pub fn mc_prune_state_depth(depth: u64) -> PruneFn {
    boxed!(move |state| {
        if state.depth > depth {
            Some(format!(
                "state depth exceeds maximum depth {depth} that is under consideration"
            ))
        } else {
            None
        }
    })
}

/// Checks that every process have not sent more that allowed number of messages
pub fn mc_prune_sent_messages_limit(max_allowed_messages: u64) -> PruneFn {
    boxed!(move |state| {
        for (node_name, node) in state.node_states.iter() {
            for (proc_name, proc) in node.iter() {
                if proc.sent_message_count > max_allowed_messages {
                    return Some(format!("too many messages sent by {node_name}:{proc_name}"));
                }
            }
        }
        None
    })
}

/// Verifies that set of messages received by process matches with expectations
pub fn mc_invariant_received_messages(node: String, proc: String, messages_expected: HashSet<String>) -> InvariantFn {
    boxed!(move |state| {
        if state.events.available_events_num() > 0 {
            return Ok(());
        }
        let mut messages_got = HashSet::<String>::default();
        let local_outbox = &state.node_states[&node][&proc].local_outbox;
        if local_outbox.len() != messages_expected.len() {
            return Err(format!(
                "{node}:{proc} received {} messages but {} expected",
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
