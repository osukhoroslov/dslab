use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dslab_core::{cast, Event, EventHandler, Id, SimulationContext};

use crate::context::Context;
use crate::events::{MessageReceived, TimerFired};
use crate::logger::{LogEntry, Logger};
use crate::message::Message;
use crate::network::Network;
use crate::process::{Process, ProcessState};

#[derive(Clone, Debug)]
pub struct EventLogEntry {
    pub time: f64,
    pub event: ProcessEvent,
}

impl EventLogEntry {
    pub fn new(time: f64, event: ProcessEvent) -> Self {
        Self { time, event }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum TimerBehavior {
    SetOnce,
    OverrideExisting,
}

#[derive(Clone, Debug)]
pub enum ProcessEvent {
    MessageSent {
        msg: Message,
        src: String,
        dest: String,
    },
    MessageReceived {
        msg: Message,
        src: String,
        dest: String,
    },
    LocalMessageSent {
        msg: Message,
    },
    LocalMessageReceived {
        msg: Message,
    },
    TimerSet {
        name: String,
        delay: f64,
        behavior: TimerBehavior,
    },
    TimerFired {
        name: String,
    },
    TimerCancelled {
        name: String,
    },
}

#[derive(Clone)]
pub(crate) struct ProcessEntry {
    pub(crate) proc_impl: Box<dyn Process>,
    pub(crate) event_log: Vec<EventLogEntry>,
    pub(crate) local_outbox: Vec<Message>,
    pub(crate) pending_timers: HashMap<String, u64>,
    pub(crate) sent_message_count: u64,
    pub(crate) received_message_count: u64,
    pub(crate) last_state: String,
}

impl ProcessEntry {
    pub fn new(proc_impl: Box<dyn Process>) -> Self {
        Self {
            proc_impl,
            event_log: Vec::new(),
            local_outbox: Vec::new(),
            pending_timers: HashMap::new(),
            sent_message_count: 0,
            received_message_count: 0,
            last_state: String::from(""),
        }
    }
}

pub struct Node {
    pub id: Id,
    pub name: String,
    processes: HashMap<String, ProcessEntry>,
    net: Rc<RefCell<Network>>,
    clock_skew: f64,
    is_crashed: bool,
    ctx: Rc<RefCell<SimulationContext>>,
    logger: Rc<RefCell<Logger>>,
    local_message_count: u64,
}

impl Node {
    pub fn new(name: String, net: Rc<RefCell<Network>>, ctx: SimulationContext, logger: Rc<RefCell<Logger>>) -> Self {
        Self {
            id: ctx.id(),
            name,
            processes: HashMap::new(),
            net,
            clock_skew: 0.,
            is_crashed: false,
            ctx: Rc::new(RefCell::new(ctx)),
            logger,
            local_message_count: 0,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_clock_skew(&mut self, clock_skew: f64) {
        self.clock_skew = clock_skew;
    }

    pub fn is_crashed(&self) -> bool {
        self.is_crashed
    }

    pub fn crash(&mut self) {
        self.processes.clear();
        self.is_crashed = true;
    }

    pub fn recover(&mut self) {
        self.is_crashed = false;
    }

    pub fn add_process(&mut self, name: &str, proc: Box<dyn Process>) {
        self.processes.insert(name.to_string(), ProcessEntry::new(proc));
    }

    pub fn get_process(&self, name: &str) -> Option<&dyn Process> {
        self.processes.get(name).map(|entry| &*entry.proc_impl)
    }

    pub fn process_names(&self) -> Vec<String> {
        self.processes.keys().cloned().collect()
    }

    pub fn set_process_state(&mut self, proc: &str, state: Box<dyn ProcessState>) {
        self.processes.get_mut(proc).unwrap().proc_impl.set_state(state);
    }

    pub fn send_local_message(&mut self, proc: String, msg: Message) {
        self.on_local_message_received(proc, msg);
    }

    pub fn read_local_messages(&mut self, proc: &str) -> Option<Vec<Message>> {
        let proc_entry = self.processes.get_mut(proc).unwrap();
        if !proc_entry.local_outbox.is_empty() {
            Some(proc_entry.local_outbox.drain(..).collect())
        } else {
            None
        }
    }

    pub fn event_log(&self, proc: &str) -> Vec<EventLogEntry> {
        self.processes[proc].event_log.clone()
    }

    pub fn max_size(&mut self, proc: &str) -> u64 {
        self.processes.get_mut(proc).unwrap().proc_impl.max_size()
    }

    pub fn sent_message_count(&self, proc: &str) -> u64 {
        self.processes[proc].sent_message_count
    }

    pub fn received_message_count(&self, proc: &str) -> u64 {
        self.processes[proc].received_message_count
    }

    fn on_local_message_received(&mut self, proc: String, msg: Message) {
        let time = self.ctx.borrow().time();
        self.logger.borrow_mut().log(LogEntry::LocalMessageReceived {
            time,
            msg_id: self.get_local_message_id(&proc, self.local_message_count),
            node: self.name.clone(),
            proc: proc.to_string(),
            msg: msg.clone(),
        });
        self.local_message_count += 1;

        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            time,
            ProcessEvent::LocalMessageReceived { msg: msg.clone() },
        ));
        let mut proc_ctx = Context::new(proc.clone(), Some(self.ctx.clone()), self.clock_skew);
        proc_entry.proc_impl.on_local_message(msg, &mut proc_ctx);
        self.handle_process_actions(proc, time, proc_ctx.actions());
    }

    fn on_message_received(&mut self, msg_id: u64, proc: String, msg: Message, from: String, from_node: String) {
        let time = self.ctx.borrow().time();
        self.logger.borrow_mut().log(LogEntry::MessageReceived {
            time,
            msg_id: msg_id.to_string(),
            src_proc: from.clone(),
            src_node: from_node,
            dest_proc: proc.clone(),
            dest_node: self.name.clone(),
            msg: msg.clone(),
        });

        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            time,
            ProcessEvent::MessageReceived {
                msg: msg.clone(),
                src: from.clone(),
                dest: proc.clone(),
            },
        ));
        proc_entry.received_message_count += 1;
        let mut proc_ctx = Context::new(proc.clone(), Some(self.ctx.clone()), self.clock_skew);
        proc_entry.proc_impl.on_message(msg, from, &mut proc_ctx);
        self.log_process_state(&proc);
        self.handle_process_actions(proc, time, proc_ctx.actions());
    }

    fn on_timer_fired(&mut self, proc: String, timer: String) {
        let time = self.ctx.borrow().time();

        let proc_entry = self.processes.get_mut(&proc).unwrap();
        if let Some(timer_id) = proc_entry.pending_timers.remove(&timer) {
            self.logger.borrow_mut().log(LogEntry::TimerFired {
                time,
                timer_id: timer_id.to_string(),
                timer_name: timer.clone(),
                node: self.name.clone(),
                proc: proc.clone(),
            });
        }
        let mut proc_ctx = Context::new(proc.clone(), Some(self.ctx.clone()), self.clock_skew);
        proc_entry.proc_impl.on_timer(timer, &mut proc_ctx);
        self.log_process_state(&proc);
        self.handle_process_actions(proc, time, proc_ctx.actions());
    }

    fn handle_process_actions(&mut self, proc: String, time: f64, actions: Vec<ProcessEvent>) {
        for action in actions {
            let proc_entry = self.processes.get_mut(&proc).unwrap();
            proc_entry.event_log.push(EventLogEntry::new(time, action.clone()));
            match action {
                ProcessEvent::MessageSent { msg, src: _, dest } => {
                    self.net.borrow_mut().send_message(msg, &proc, &dest);
                    proc_entry.sent_message_count += 1;
                }
                ProcessEvent::LocalMessageSent { msg } => {
                    proc_entry.local_outbox.push(msg.clone());

                    self.logger.borrow_mut().log(LogEntry::LocalMessageSent {
                        time,
                        msg_id: self.get_local_message_id(&proc, self.local_message_count),
                        node: self.name.clone(),
                        proc: proc.to_string(),
                        msg: msg.clone(),
                    });
                    self.local_message_count += 1;
                }
                ProcessEvent::TimerSet { name, delay, behavior } => {
                    if behavior == TimerBehavior::OverrideExisting || !proc_entry.pending_timers.contains_key(&name) {
                        let event = TimerFired {
                            timer: name.clone(),
                            proc: proc.clone(),
                        };
                        let event_id = self.ctx.borrow_mut().emit_self(event, delay);
                        proc_entry.pending_timers.insert(name.clone(), event_id);

                        self.logger.borrow_mut().log(LogEntry::TimerSet {
                            time,
                            timer_id: event_id.to_string(),
                            timer_name: name.clone(),
                            node: self.name.clone(),
                            proc: proc.clone(),
                            delay,
                        });
                    }
                }
                ProcessEvent::TimerCancelled { name } => {
                    if let Some(event_id) = proc_entry.pending_timers.remove(&name) {
                        self.logger.borrow_mut().log(LogEntry::TimerCancelled {
                            time,
                            timer_id: event_id.to_string(),
                            timer_name: name.clone(),
                            node: self.name.clone(),
                            proc: proc.clone(),
                        });

                        self.ctx.borrow_mut().cancel_event(event_id);
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn processes(&self) -> HashMap<String, ProcessEntry> {
        self.processes.clone()
    }

    fn get_local_message_id(&self, proc: &str, local_message_count: u64) -> String {
        format!("{}-{}-{}", self.name, proc, local_message_count)
    }

    fn log_process_state(&mut self, proc: &str) {
        let mut proc_entry = self.processes.get_mut(proc).unwrap();
        let state = format!("{:?}", proc_entry.proc_impl.state());
        if state != proc_entry.last_state {
            proc_entry.last_state = state.clone();
            self.logger.borrow_mut().log(LogEntry::ProcessStateUpdated {
                time: self.ctx.borrow().time(),
                node: self.name.clone(),
                proc: proc.to_string(),
                state,
            });
        }
    }
}

impl EventHandler for Node {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            MessageReceived {
                id,
                msg,
                src,
                src_node,
                dest,
                dest_node: _,
            } => {
                self.on_message_received(id, dest, msg, src, src_node);
            }
            TimerFired { proc, timer } => {
                self.on_timer_fired(proc, timer);
            }
        })
    }
}
