use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;

use dslab_core::{cast, Event, EventHandler, Id, SimulationContext};

use crate::context::Context;
use crate::events::{MessageReceived, TimerFired};
use crate::message::Message;
use crate::network::Network;
use crate::process::Process;
use crate::util::t;

#[derive(Clone)]
pub struct EventLogEntry {
    pub time: f64,
    pub event: ProcessEvent,
}

impl EventLogEntry {
    pub fn new(time: f64, event: ProcessEvent) -> Self {
        Self { time, event }
    }
}

#[derive(Clone)]
pub enum ProcessEvent {
    MessageSent { msg: Message, src: String, dest: String },
    MessageReceived { msg: Message, src: String, dest: String },
    LocalMessageSent { msg: Message },
    LocalMessageReceived { msg: Message },
    TimerSet { name: String, delay: f64 },
    TimerFired { name: String },
    TimerCancelled { name: String },
}

struct ProcessEntry {
    proc_impl: Box<dyn Process>,
    event_log: Vec<EventLogEntry>,
    local_outbox: Vec<Message>,
    pending_timers: HashMap<String, u64>,
    sent_message_count: u64,
    received_message_count: u64,
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
        }
    }
}

pub struct Node {
    #[allow(dead_code)]
    id: Id,
    #[allow(dead_code)]
    name: String,
    processes: HashMap<String, ProcessEntry>,
    net: Rc<RefCell<Network>>,
    clock_skew: f64,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl Node {
    pub fn new(name: String, net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            name,
            processes: HashMap::new(),
            net,
            clock_skew: 0.,
            ctx: Rc::new(RefCell::new(ctx)),
        }
    }

    pub fn set_clock_skew(&mut self, clock_skew: f64) {
        self.clock_skew = clock_skew;
    }

    pub fn add_process(&mut self, name: &str, proc: Box<dyn Process>) {
        self.processes.insert(name.to_string(), ProcessEntry::new(proc));
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
        t!(format!("{:>9.3} {:>10} <<< {:<10} {:?}", time, proc, "local", msg).cyan());
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            time,
            ProcessEvent::LocalMessageReceived { msg: msg.clone() },
        ));
        let mut proc_ctx = Context::new(proc.clone(), self.ctx.clone(), self.clock_skew);
        proc_entry.proc_impl.on_local_message(msg, &mut proc_ctx);
        self.handle_process_actions(proc, time, proc_ctx.actions());
    }

    fn on_message_received(&mut self, proc: String, msg: Message, from: String) {
        let time = self.ctx.borrow().time();
        t!("{:>9.3} {:>10} <-- {:<10} {:?}", time, proc, from, msg);
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
        let mut proc_ctx = Context::new(proc.clone(), self.ctx.clone(), self.clock_skew);
        proc_entry.proc_impl.on_message(msg, from, &mut proc_ctx);
        self.handle_process_actions(proc, time, proc_ctx.actions());
    }

    fn on_timer_fired(&mut self, proc: String, timer: String) {
        let time = self.ctx.borrow().time();
        t!(format!("{:>9.3} {:>10} !-- {:<10}", time, proc, timer).yellow());
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        proc_entry.pending_timers.remove(&timer);
        let mut proc_ctx = Context::new(proc.clone(), self.ctx.clone(), self.clock_skew);
        proc_entry.proc_impl.on_timer(timer, &mut proc_ctx);
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
                    proc_entry.local_outbox.push(msg);
                }
                ProcessEvent::TimerSet { name, delay } => {
                    assert!(
                        !proc_entry.pending_timers.contains_key(&name),
                        "Timer \"{}\" is already set by process \"{}\" (active timer names should be unique!)",
                        name,
                        proc
                    );
                    let event = TimerFired {
                        timer: name.clone(),
                        proc: proc.clone(),
                    };
                    let event_id = self.ctx.borrow_mut().emit_self(event, delay);
                    proc_entry.pending_timers.insert(name, event_id);
                }
                ProcessEvent::TimerCancelled { name } => {
                    if let Some(event_id) = proc_entry.pending_timers.remove(&name) {
                        self.ctx.borrow_mut().cancel_event(event_id);
                    }
                }
                _ => {}
            }
        }
    }
}

impl EventHandler for Node {
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
