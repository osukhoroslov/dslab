use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::SimulationContext;

use crate::message::Message;
use crate::node::{ProcessEvent, TimerBehavior};

pub struct Context {
    proc_name: String,
    time: f64,
    sim_ctx: Option<Rc<RefCell<SimulationContext>>>,
    actions: Vec<ProcessEvent>,
}

impl Context {
    pub fn from_simulation(proc_name: String, sim_ctx: Rc<RefCell<SimulationContext>>, clock_skew: f64) -> Self {
        let time = sim_ctx.borrow().time() + clock_skew;
        Self {
            proc_name,
            time,
            sim_ctx: Some(sim_ctx),
            actions: Vec::new(),
        }
    }

    pub fn from_mc(proc_name: String, state_depth: u64) -> Self {
        // this ensures every step of model checking
        // simulation represents 0.1s period
        // it makes time value look more natual and closer to simulation time
        let time = state_depth as f64 / 10.0;
        Self {
            proc_name,
            time,
            sim_ctx: None,
            actions: Vec::new(),
        }
    }

    pub fn time(&self) -> f64 {
        self.time
    }

    pub fn rand(&mut self) -> f64 {
        self.sim_ctx.as_ref().expect("sim_ctx is None").borrow_mut().rand()
    }

    pub fn send(&mut self, msg: Message, dest: String) {
        self.actions.push(ProcessEvent::MessageSent {
            msg,
            src: self.proc_name.clone(),
            dest,
        });
    }

    pub fn send_local(&mut self, msg: Message) {
        self.actions.push(ProcessEvent::LocalMessageSent { msg });
    }

    pub fn set_timer(&mut self, name: &str, delay: f64) {
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::OverrideExisting,
        });
    }

    pub fn set_timer_once(&mut self, name: &str, delay: f64) {
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::SetOnce,
        });
    }

    pub fn cancel_timer(&mut self, name: &str) {
        self.actions
            .push(ProcessEvent::TimerCancelled { name: name.to_string() });
    }

    pub(crate) fn actions(&mut self) -> Vec<ProcessEvent> {
        self.actions.drain(..).collect()
    }
}
