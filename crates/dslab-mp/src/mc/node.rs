use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, Id, SimulationContext};

use crate::context::Context;
use crate::events::{MessageReceived, TimerFired};
use crate::mc::network::McNetwork;
use crate::message::Message;
use crate::node::{EventLogEntry, ProcessEntry, ProcessEvent, TimerBehavior};
use crate::process::ProcessState;

pub struct ProcessEntryState {
    pub process_state: Box<dyn ProcessState>,
    pub event_log: Vec<EventLogEntry>,
    pub local_outbox: Vec<Message>,
    pub pending_timers: HashMap<String, u64>,
    pub sent_message_count: u64,
    pub received_message_count: u64,
}

pub struct McNodeState {
    pub process_entry_states: HashMap<String, ProcessEntryState>,
}

pub struct McNode {
    id: Id,
    name: String,
    processes: HashMap<String, ProcessEntry>,
    net: Rc<RefCell<McNetwork>>,
}

impl McNode {
    pub(crate) fn new(
        id: Id,
        name: String,
        processes: HashMap<String, ProcessEntry>,
        net: Rc<RefCell<McNetwork>>,
    ) -> Self {
        Self {
            id,
            name,
            processes,
            net,
        }
    }

    fn create_ctx(proc_name: String) -> Context {
        Context::new(proc_name, None, 0.0)
    }

    pub fn on_message_received(&mut self, proc: String, msg: Message, from: String) {
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

        let mut proc_ctx = Self::create_ctx(proc.clone());
        proc_entry.proc_impl.on_message(msg, from, &mut proc_ctx);
        self.handle_process_actions(proc, 0.0, proc_ctx.actions());
    }

    pub fn on_timer_fired(&mut self, proc: String, timer: String) {
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.pending_timers.remove(&timer);

        let mut proc_ctx = Self::create_ctx(proc.clone());
        proc_entry.proc_impl.on_timer(timer, &mut proc_ctx);
        self.handle_process_actions(proc, 0.0, proc_ctx.actions());
    }

    fn handle_process_actions(&mut self, proc: String, time: f64, actions: Vec<ProcessEvent>) {
        for action in actions {
            let proc_entry = self.processes.get_mut(&proc).unwrap();
            proc_entry.event_log.push(EventLogEntry::new(time, action.clone()));
            match action {
                ProcessEvent::MessageSent { msg, src, dest } => {
                    if !self.net.borrow_mut().send_message(msg, src, dest, self.id) {
                        return;
                    }
                    proc_entry.sent_message_count += 1;
                }
                ProcessEvent::LocalMessageSent { msg } => {
                    proc_entry.local_outbox.push(msg);
                }
                ProcessEvent::TimerSet {
                    name,
                    delay: _delay,
                    behavior,
                } => {
                    if behavior == TimerBehavior::OverrideExisting || !proc_entry.pending_timers.contains_key(&name) {
                        let event_id = self.net.borrow_mut().set_timer(name.clone(), proc.clone(), self.id);
                        proc_entry.pending_timers.insert(name, event_id);
                    }
                }
                // TODO: Add handling of timer cancellation after adding of event dependencies resolver
                /* ProcessEvent::TimerCancelled { name } => {
                    if let Some(event_id) = proc_entry.pending_timers.remove(&name) {
                        self.ctx.borrow_mut().cancel_event(event_id);
                    }
                }*/
                _ => {}
            }
        }
    }

    pub fn get_state(&self) -> McNodeState {
        let mut state = McNodeState {
            process_entry_states: HashMap::new(),
        };
        for (proc, proc_entry) in &self.processes {
            *state.process_entry_states.get_mut(proc).unwrap() = ProcessEntryState {
                process_state: proc_entry.proc_impl.state(),
                event_log: proc_entry.event_log.clone(),
                local_outbox: proc_entry.local_outbox.clone(),
                pending_timers: proc_entry.pending_timers.clone(),
                sent_message_count: proc_entry.sent_message_count,
                received_message_count: proc_entry.received_message_count,
            }
        }
        state
    }

    pub fn set_state(&mut self, state: McNodeState) {
        for (proc, proc_entry_state) in state.process_entry_states {
            if let Some(proc_entry) = self.processes.get_mut(&proc) {
                proc_entry.proc_impl.set_state(proc_entry_state.process_state);
                proc_entry.event_log = proc_entry_state.event_log;
                proc_entry.local_outbox = proc_entry_state.local_outbox;
                proc_entry.pending_timers = proc_entry_state.pending_timers;
                proc_entry.sent_message_count = proc_entry_state.sent_message_count;
                proc_entry.received_message_count = proc_entry_state.received_message_count;
            }
        }
    }
}

impl EventHandler for McNode {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            MessageReceived { msg, src, dest } => {
                self.on_message_received(dest, msg, src);
            }
            TimerFired { proc, timer } => {
                self.on_timer_fired(proc, timer);
            }
        })
    }
}
