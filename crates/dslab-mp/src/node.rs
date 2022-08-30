use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use colored::*;

use dslab_core::{cast, Event, EventHandler, Id, SimulationContext};

use crate::context::Context;
use crate::events::{LocalMessageReceived, MessageReceived, TimerFired};
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
    proc: Rc<RefCell<dyn Process>>,
    event_log: Vec<EventLogEntry>,
    local_outbox: Vec<Message>,
    pending_timers: HashMap<String, u64>,
}

impl ProcessEntry {
    pub fn new(proc: Rc<RefCell<dyn Process>>) -> Self {
        Self {
            proc,
            event_log: Vec::new(),
            local_outbox: Vec::new(),
            pending_timers: HashMap::new(),
        }
    }
}

pub struct Node {
    #[allow(dead_code)]
    id: Id,
    processes: HashMap<String, ProcessEntry>,
    net: Rc<RefCell<Network>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl Node {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            processes: HashMap::new(),
            net,
            ctx: Rc::new(RefCell::new(ctx)),
        }
    }

    pub fn add_proc(&mut self, proc: Rc<RefCell<dyn Process>>, proc_id: String) {
        self.processes.insert(proc_id, ProcessEntry::new(proc));
    }

    pub fn send_local(&mut self, msg: Message, proc: String) {
        let event = LocalMessageReceived {
            msg,
            dest: proc.clone(),
        };
        self.ctx.borrow_mut().emit_self(event, 0.);
    }

    pub fn event_log(&self, proc: String) -> Vec<EventLogEntry> {
        self.processes.get(&proc).unwrap().event_log.clone()
    }

    pub fn read_local_messages(&mut self, proc: String) -> Option<Vec<Message>> {
        let proc_entry = self.processes.get_mut(&proc).unwrap();
        if proc_entry.local_outbox.len() > 0 {
            Some(proc_entry.local_outbox.drain(..).collect())
        } else {
            None
        }
    }

    fn on_message_received(&mut self, msg: Message, src: String, dest: String) {
        let time = self.ctx.borrow().time();
        t!("{:>9.3} {:>10} <-- {:<10} {:?}", time, dest, src, msg);
        let proc_entry = self.processes.get_mut(&dest).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            time,
            ProcessEvent::MessageReceived {
                msg: msg.clone(),
                src: src.clone(),
                dest: dest.clone(),
            },
        ));
        let mut proc_ctx = Context::new(dest.clone(), self.ctx.clone());
        proc_entry.proc.borrow_mut().on_message(msg, src, &mut proc_ctx);
        self.handle_process_actions(dest, time, proc_ctx.actions());
    }

    fn on_local_message_received(&mut self, msg: Message, dest: String) {
        let time = self.ctx.borrow().time();
        t!(format!("{:>9.3} {:>10} <<< {:<10} {:?}", time, dest, "local", msg).cyan());
        let proc_entry = self.processes.get_mut(&dest).unwrap();
        proc_entry.event_log.push(EventLogEntry::new(
            time,
            ProcessEvent::LocalMessageReceived { msg: msg.clone() },
        ));
        let mut proc_ctx = Context::new(dest.clone(), self.ctx.clone());
        proc_entry.proc.borrow_mut().on_local_message(msg, &mut proc_ctx);
        self.handle_process_actions(dest, time, proc_ctx.actions());
    }

    fn on_timer_fired(&mut self, timer_name: String, proc_name: String) {
        let time = self.ctx.borrow().time();
        t!(format!("{:>9.3} {:>10} !-- {:<10}", time, proc_name, timer_name).magenta());
        let proc_entry = self.processes.get_mut(&proc_name).unwrap();
        proc_entry.pending_timers.remove(&timer_name);
        let mut proc_ctx = Context::new(proc_name.clone(), self.ctx.clone());
        proc_entry.proc.borrow_mut().on_timer(timer_name, &mut proc_ctx);
        self.handle_process_actions(proc_name, time, proc_ctx.actions());
    }

    fn handle_process_actions(&mut self, proc_name: String, time: f64, actions: Vec<ProcessEvent>) {
        for action in actions {
            let proc_entry = self.processes.get_mut(&proc_name).unwrap();
            proc_entry.event_log.push(EventLogEntry::new(time, action.clone()));
            match action {
                ProcessEvent::MessageSent { msg, src: _, dest } => {
                    if proc_name == dest {
                        let event = MessageReceived {
                            msg,
                            src: proc_name.clone(),
                            dest: dest.clone(),
                        };
                        self.ctx.borrow_mut().emit_self(event, 0.0);
                    } else {
                        self.net.borrow_mut().send_message(msg, proc_name.clone(), dest.clone());
                    }
                }
                ProcessEvent::LocalMessageSent { msg } => {
                    proc_entry.local_outbox.push(msg);
                }
                ProcessEvent::TimerSet { name, delay } => {
                    assert!(
                        !proc_entry.pending_timers.contains_key(&name),
                        "Timer \"{}\" is already set by process \"{}\" (active timer ids should be unique!)",
                        name,
                        proc_name
                    );
                    let event = TimerFired {
                        timer_name: name.clone(),
                        proc_name: proc_name.clone(),
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
                self.on_message_received(msg, src, dest);
            }
            LocalMessageReceived { msg, dest } => {
                self.on_local_message_received(msg, dest);
            }
            TimerFired { timer_name, proc_name } => {
                self.on_timer_fired(timer_name, proc_name);
            }
        })
    }
}
