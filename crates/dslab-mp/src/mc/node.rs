use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::context::Context;
use crate::mc::events::{McEvent, McTime};
use crate::mc::network::McNetwork;
use crate::message::Message;
use crate::node::{EventLogEntry, ProcessEntry, ProcessEvent, TimerBehavior};
use crate::process::ProcessState;

#[derive(Debug, Clone)]
pub struct ProcessEntryState {
    pub proc_state: Rc<dyn ProcessState>,
    pub event_log: Vec<EventLogEntry>,
    pub local_outbox: Vec<Message>,
    pub pending_timers: HashMap<String, u64>,
    pub sent_message_count: u64,
    pub received_message_count: u64,
}

impl Hash for ProcessEntryState {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.proc_state.hash_with_dyn(hasher);
        self.local_outbox.hash(hasher);
    }
}

impl PartialEq for ProcessEntryState {
    fn eq(&self, other: &Self) -> bool {
        let equal_process_states = self.proc_state.eq_with_dyn(&*other.proc_state);
        equal_process_states && self.local_outbox == other.local_outbox
    }
}

impl Eq for ProcessEntryState {
    fn assert_receiver_is_total_eq(&self) {}
}

impl ProcessEntry {
    fn get_state(&self) -> ProcessEntryState {
        ProcessEntryState {
            proc_state: self.proc_impl.state(),
            event_log: self.event_log.clone(),
            local_outbox: self.local_outbox.clone(),
            pending_timers: self.pending_timers.clone(),
            sent_message_count: self.sent_message_count,
            received_message_count: self.received_message_count,
        }
    }

    fn set_state(&mut self, state: ProcessEntryState) {
        self.proc_impl.set_state(state.proc_state);
        self.event_log = state.event_log;
        self.local_outbox = state.local_outbox;
        self.pending_timers = state.pending_timers;
        self.sent_message_count = state.sent_message_count;
        self.received_message_count = state.received_message_count;
    }
}

pub type McNodeState = BTreeMap<String, ProcessEntryState>;

pub struct McNode {
    processes: HashMap<String, ProcessEntry>,
    net: Rc<RefCell<McNetwork>>,
    clock_skew: f64,
}

impl McNode {
    pub(crate) fn new(processes: HashMap<String, ProcessEntry>, net: Rc<RefCell<McNetwork>>, clock_skew: f64) -> Self {
        Self {
            processes,
            net,
            clock_skew,
        }
    }

    pub fn on_message_received(
        &mut self,
        proc: String,
        msg: Message,
        from: String,
        time: f64,
        random_seed: u64,
    ) -> Vec<McEvent> {
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            0.0,
            ProcessEvent::MessageReceived {
                msg: msg.clone(),
                src: from.clone(),
                dest: proc.clone(),
            },
        ));
        proc_entry.received_message_count += 1;

        let mut proc_ctx = Context::basic(proc.to_string(), time, self.clock_skew, random_seed);
        proc_entry.proc_impl.on_message(msg, from, &mut proc_ctx);
        self.handle_process_actions(proc, 0.0, proc_ctx.actions())
    }

    pub fn on_timer_fired(&mut self, proc: String, timer: String, time: f64, random_seed: u64) -> Vec<McEvent> {
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.pending_timers.remove(&timer);

        let mut proc_ctx = Context::basic(proc.to_string(), time, self.clock_skew, random_seed);
        proc_entry.proc_impl.on_timer(timer, &mut proc_ctx);
        self.handle_process_actions(proc, 0.0, proc_ctx.actions())
    }

    pub fn on_local_message_received(
        &mut self,
        proc: String,
        msg: Message,
        time: f64,
        random_seed: u64,
    ) -> Vec<McEvent> {
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        let mut proc_ctx = Context::basic(proc.to_string(), time, self.clock_skew, random_seed);
        proc_entry.proc_impl.on_local_message(msg, &mut proc_ctx);
        self.handle_process_actions(proc, time, proc_ctx.actions())
    }

    pub fn get_state(&self) -> McNodeState {
        self.processes
            .iter()
            .map(|(proc, entry)| (proc.clone(), entry.get_state()))
            .collect()
    }

    pub fn set_state(&mut self, state: McNodeState) {
        for (proc, state) in state {
            self.processes.get_mut(&proc).unwrap().set_state(state);
        }
    }

    fn handle_process_actions(&mut self, proc: String, time: f64, actions: Vec<ProcessEvent>) -> Vec<McEvent> {
        let mut new_events = Vec::new();
        for action in actions {
            let proc_entry = self.processes.get_mut(&proc).unwrap();
            proc_entry.event_log.push(EventLogEntry::new(time, action.clone()));
            match action {
                ProcessEvent::MessageSent { msg, src, dest } => {
                    let event = self.net.borrow_mut().send_message(msg, src, dest);
                    new_events.push(event);
                    proc_entry.sent_message_count += 1;
                }
                ProcessEvent::LocalMessageSent { msg } => {
                    proc_entry.local_outbox.push(msg);
                }
                ProcessEvent::TimerSet { name, delay, behavior } => {
                    if behavior == TimerBehavior::OverrideExisting || !proc_entry.pending_timers.contains_key(&name) {
                        let event = McEvent::TimerFired {
                            timer: name.clone(),
                            proc: proc.clone(),
                            timer_delay: McTime::from(delay),
                        };
                        new_events.push(event);
                        // event_id is 0 since it is not used in model checking
                        proc_entry.pending_timers.insert(name, 0);
                    }
                }
                ProcessEvent::TimerCancelled { name } => {
                    proc_entry.pending_timers.remove(&name);
                    let event = McEvent::TimerCancelled {
                        timer: name,
                        proc: proc.clone(),
                    };
                    new_events.push(event);
                }
                _ => {}
            }
        }
        new_events
    }
}
