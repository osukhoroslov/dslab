//! Predicates that can be used for configuration in model checker

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

/// Checks if n messages were received by a given process.
pub fn mc_goal_got_n_local_messages(node: String, proc: String, n: u64) -> GoalFn {
    boxed!(move |state| {
        if state.node_states[&node][&proc].local_outbox.len() == n as usize {
            Some(format!("{} produced {} local messages", proc, n))
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
            Some(format!("state depth exceeds maximum depth {depth} that is under consideration"))
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
                    return Some(format!("too many messages sent by {}:{}", node_name, proc_name));
                }
            }
        }
        None
    })
}
