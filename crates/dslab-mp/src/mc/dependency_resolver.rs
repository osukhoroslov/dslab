use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use crate::mc::events::{McEventId, McDuration};

type BidirectionalMapping = (
    BTreeMap<McDuration, BTreeSet<McEventId>>,
    BTreeMap<McEventId, McDuration>,
);

/// Tracks and enforces dependencies between the TimerFired events on the same node.
/// Any timer with time T should be fired before any timer with time T+x.  
/// Also keeps track of messages in stable network that can block long timers.
#[derive(Default, Clone, Hash, Eq, PartialEq)]
pub struct DependencyResolver {
    proc_timers: BTreeMap<String, BidirectionalMapping>,
    pending_messages: BTreeMap<String, BidirectionalMapping>,
    event_to_proc: BTreeMap<McEventId, String>,
}

impl DependencyResolver {
    fn get_all_available_events(&self, proc: &String) -> BTreeSet<McEventId> {
        let messages_iter = self.pending_messages[proc].1.keys().map(|x| *x);
        if let Some((time, timers)) = self.get_min_time_timers(proc) {
            if !self.get_min_time_message(proc).map_or(false, |t| t < time) {
                return BTreeSet::from_iter(messages_iter.chain(timers.clone().into_iter()));
            }
        }
        BTreeSet::from_iter(messages_iter)
    }

    pub fn add_timer(
        &mut self,
        proc: String,
        time: McDuration,
        event: McEventId,
    ) -> (bool, Option<BTreeSet<McEventId>>) {
        self.add_event_to_node_mapping(proc.clone(), event);

        Self::create_event_mappings(&mut self.proc_timers, &proc, event, time);

        let timer_group = &self.proc_timers[&proc].0;
        let prev_time = timer_group.range(..time).next_back();
        let mut is_available = prev_time.is_none();

        if let Some(timer) = self.get_min_time_message(&proc) {
            if *timer < time {
                is_available = false;
            }
        }

        let next_time = timer_group.range((Excluded(time), Unbounded)).next();
        let blocked_events = next_time.map(|e| e.1).cloned();

        (is_available, blocked_events)
    }

    pub fn add_message(&mut self, proc: String, time: McDuration, event: McEventId) -> Option<BTreeSet<McEventId>> {
        self.add_event_to_node_mapping(proc.clone(), event);
        Self::create_event_mappings(&mut self.pending_messages, &proc, event, time);

        let timers = self.proc_timers.entry(proc).or_default();
        if let Some((timer, first_group)) = timers.0.iter().next() {
            if *timer > McDuration::from(time) {
                return Some(first_group.clone());
            }
        }
        None
    }

    pub fn cancel_timer(&mut self, proc: String, event: McEventId) -> BTreeSet<McEventId> {
        let timers = self.proc_timers.entry(proc.clone()).or_default();
        let time = timers.1[&event];
        timers.0.get_mut(&time).unwrap().remove(&event);
        self.event_to_proc.remove(&event);
        self.get_all_available_events(&proc)
    }

    fn add_event_to_node_mapping(&mut self, proc: String, event: McEventId) {
        assert!(
            self.event_to_proc.insert(event, proc).is_none(),
            "duplicate McEventId not allowed"
        );
    }

    fn create_event_mappings(
        proc_mapping: &mut BTreeMap<String, BidirectionalMapping>,
        proc: &String,
        id: McEventId,
        time: McDuration,
    ) {
        let mapping = proc_mapping.entry(proc.clone()).or_default();
        mapping.0.entry(McDuration::from(time)).or_default().insert(id);
        mapping.1.insert(id, time);
    }

    fn get_min_time_message(&self, proc: &String) -> Option<&McDuration> {
        if let Some(messages) = self.pending_messages.get(proc) {
            messages.0.iter().next().map(|x| x.0)
        } else {
            None
        }
    }

    fn get_min_time_timers(&self, proc: &String) -> Option<(&McDuration, &BTreeSet<McEventId>)> {
        if let Some(timers) = self.proc_timers.get(proc) {
            if let Some(timer_group) = timers.0.iter().next() {
                Some(timer_group)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn pop(&mut self, event: McEventId) -> BTreeSet<McEventId> {
        let proc = self.event_to_proc.remove(&event).unwrap();
        let messages = self.pending_messages.entry(proc.clone()).or_default();
        if let Some(time) = messages.1.remove(&event) {
            let message_group = messages.0.get_mut(&time).unwrap();
            message_group.remove(&event);
            if message_group.is_empty() {
                messages.0.remove(&time);
            }
            self.get_all_available_events(&proc)
        } else {
            // check that timer is not blocked by messages
            let timers = self.proc_timers.get(&proc).unwrap();
            let (timer_time, _) = timers.0.iter().next().unwrap();
            let timer_time = *timer_time;
            if let Some(min_message_time) = self.get_min_time_message(&proc) {
                assert!(timer_time <= *min_message_time, "timer is blocked by message");
            }

            // check that timer is really first timer
            let timers = self.proc_timers.get_mut(&proc).unwrap();
            let (min_time, events) = timers.0.iter_mut().next().unwrap();
            let min_time = *min_time;
            assert!(events.remove(&event), "event to pop was not first in queue");
            if events.is_empty() {
                timers.0.remove(&min_time);
            }

            self.get_all_available_events(&proc)
        }
    }
}
