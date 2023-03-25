use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::mc::events::{McEventId, McTime};

type BlockingEvents = BTreeMap<McEventId, BTreeSet<McEventId>>;

/// Tracks and enforces dependencies between the TimerFired events on the same node.
/// Any timer with time T should be fired before any timer with time T+x.  
/// Also keeps track of messages in stable network that can block long timers.
#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct DependencyResolver {
    pending_timers: BTreeMap<String, BlockingEvents>,
    event_to_proc: BTreeMap<McEventId, String>,
    event_to_time: BTreeMap<McEventId, McTime>,
}

impl DependencyResolver {
    fn _get_all_available_events(&self) -> BTreeSet<McEventId> {
        let mut res = BTreeSet::new();
        for timers in self.pending_timers.values() {
            for (id, blockers) in timers {
                if blockers.is_empty() {
                    res.insert(*id);
                }
            }
        }
        res
    }

    pub fn add_timer(&mut self, proc: String, time: McTime, event: McEventId) -> bool {
        self.add_event_mappings(proc.clone(), event, time);

        let timers = self.pending_timers.entry(proc).or_default();
        let mut blockers = BTreeSet::default();
        for id in timers.keys() {
            if self.event_to_time[id] <= time {
                blockers.insert(*id);
            }
        }
        let is_available = blockers.is_empty();
        timers.insert(event, blockers);
        is_available
    }

    pub fn cancel_timer(&mut self, _: String, event: McEventId) -> BTreeSet<McEventId> {
        self.pop(event)
    }

    fn add_event_mappings(&mut self, proc: String, event: McEventId, time: McTime) {
        assert!(
            self.event_to_proc.insert(event, proc).is_none(),
            "duplicate McEventId not allowed"
        );
        assert!(
            self.event_to_time.insert(event, time).is_none(),
            "duplicate McEventId not allowed"
        );
    }

    pub fn pop(&mut self, event: McEventId) -> BTreeSet<McEventId> {
        let proc = self.event_to_proc.remove(&event).unwrap();
        self.event_to_time.remove(&event).unwrap();

        let mut new_available = BTreeSet::default();

        let timers = self.pending_timers.get_mut(&proc).unwrap();
        assert!(timers.remove(&event).is_some());
        for (id, blockers) in timers.iter_mut() {
            blockers.remove(&event);
            if blockers.is_empty() {
                new_available.insert(*id);
            }
        }

        new_available
    }
}
