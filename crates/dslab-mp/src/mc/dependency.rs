use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::mc::events::{McEventId, McTime};
use crate::message::Message;

/// Tracks and enforces dependencies between events.
///
/// Dependency here actually means an ordering constraint, i.e. event A must happen before event B.
/// Knowing such dependencies allows to reduce the state space for model checking by excluding executions that
/// violate such constraints, e.g. where B happened before A.
///
/// Currently it supports tracking dependencies between the TimerFired events within a single process.
/// A timer is blocked by (must happen after) other previously set and active timers with less or equal delay,
/// because it is not possible to "overtake" such timers (we assume that timers with equal firing time are processed
/// in the order of their activation).
/// The inverse is generally not true - a new timer cannot block any existing timer with larger delay, because
/// we do not know exactly the time moments when these timers were activated.
#[derive(Default, Clone, Hash, Eq, PartialEq, Debug)]
pub struct DependencyResolver {
    timers: BTreeMap<McEventId, TimerInfo>,
    messages: BTreeMap<Message, VecDeque<McEventId>>,
    proc_timers: BTreeMap<String, BTreeSet<McEventId>>,
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
struct TimerInfo {
    proc: String,
    delay: McTime,
    blockers: BTreeSet<McEventId>,
}

impl DependencyResolver {
    pub fn add_timer(&mut self, proc: String, delay: McTime, event_id: McEventId) -> bool {
        let proc_timers = self.proc_timers.entry(proc.clone()).or_default();
        let mut blockers = BTreeSet::default();
        for id in proc_timers.iter() {
            if self.timers[id].delay <= delay {
                blockers.insert(*id);
            }
        }
        let is_available = blockers.is_empty();
        assert!(
            self.timers
                .insert(event_id, TimerInfo { proc, delay, blockers })
                .is_none(),
            "event with such id already exists"
        );
        proc_timers.insert(event_id);
        is_available
    }

    pub fn remove_timer(&mut self, event_id: McEventId) -> BTreeSet<McEventId> {
        let timer = self.timers.remove(&event_id).unwrap();
        let proc_timers = self.proc_timers.get_mut(&timer.proc).unwrap();
        assert!(proc_timers.remove(&event_id));

        let mut unblocked = BTreeSet::default();
        for other_id in proc_timers.iter() {
            let other_blockers = &mut self.timers.get_mut(other_id).unwrap().blockers;
            other_blockers.remove(&event_id);
            if other_blockers.is_empty() {
                unblocked.insert(*other_id);
            }
        }
        unblocked
    }

    pub fn add_message(&mut self, msg: Message, event_id: McEventId) -> bool {
        let vec_ref = self.messages.entry(msg).or_default();
        vec_ref.push_back(event_id);
        vec_ref.len() == 1
    }

    pub fn remove_message(&mut self, msg: Message) -> Option<McEventId> {
        let ids = self.messages.get_mut(&msg).unwrap();
        ids.pop_front();
        if !ids.is_empty() {
            Some(ids[0])
        } else {
            None
        }
    }
}
