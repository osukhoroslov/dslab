//! Process context.

use std::cell::RefCell;
use std::rc::Rc;

use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use simcore::SimulationContext;

use crate::message::Message;
use crate::node::{ProcessEvent, TimerBehavior};

/// Proxy for interaction of a process with the system.
pub struct Context {
    proc_name: String,
    time: f64,
    rng: Box<dyn RandomProvider>,
    actions: Vec<ProcessEvent>,
}

trait RandomProvider {
    fn rand(&mut self) -> f64;
}

struct SimulationRng {
    sim_ctx: Rc<RefCell<SimulationContext>>,
}

impl RandomProvider for SimulationRng {
    fn rand(&mut self) -> f64 {
        self.sim_ctx.borrow().rand()
    }
}

impl RandomProvider for Pcg64 {
    fn rand(&mut self) -> f64 {
        self.gen_range(0.0..1.0)
    }
}

impl Context {
    /// Creates a context used in simulation mode.
    pub fn from_simulation(proc_name: String, sim_ctx: Rc<RefCell<SimulationContext>>, clock_skew: f64) -> Self {
        let time = sim_ctx.borrow().time() + clock_skew;
        Self {
            proc_name,
            time,
            rng: Box::new(SimulationRng { sim_ctx }),
            actions: Vec::new(),
        }
    }

    /// Creates a context used in model checking mode.
    pub fn basic(proc_name: String, time: f64, clock_skew: f64, random_seed: u64) -> Self {
        Self {
            proc_name,
            time: time + clock_skew,
            rng: Box::new(Pcg64::seed_from_u64(random_seed)),
            actions: Vec::new(),
        }
    }

    /// Returns the current time from the local node clock.
    pub fn time(&self) -> f64 {
        self.time
    }

    /// Returns a random float in the range `[0, 1)`.
    pub fn rand(&mut self) -> f64 {
        self.rng.as_mut().rand()
    }

    /// Sends a message to a process.
    pub fn send(&mut self, msg: Message, dst: String) {
        assert!(
            msg.tip.len() <= 50,
            "Message type length exceeds the limit of 50 characters"
        );
        self.actions.push(ProcessEvent::MessageSent {
            msg,
            src: self.proc_name.clone(),
            dst,
        });
    }

    /// Sends a local message.
    pub fn send_local(&mut self, msg: Message) {
        assert!(
            msg.tip.len() <= 50,
            "Message type length exceeds the limit of 50 characters"
        );
        self.actions.push(ProcessEvent::LocalMessageSent { msg });
    }

    /// Sets a timer with overriding delay of existing active timer.
    pub fn set_timer(&mut self, name: &str, delay: f64) {
        assert!(name.len() <= 50, "Timer name length exceeds the limit of 50 characters");
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::OverrideExisting,
        });
    }

    /// Sets a timer without overriding delay of existing active timer.
    pub fn set_timer_once(&mut self, name: &str, delay: f64) {
        assert!(name.len() <= 50, "Timer name length exceeds the limit of 50 characters");
        self.actions.push(ProcessEvent::TimerSet {
            name: name.to_string(),
            delay,
            behavior: TimerBehavior::SetOnce,
        });
    }

    /// Cancels a timer.
    pub fn cancel_timer(&mut self, name: &str) {
        self.actions
            .push(ProcessEvent::TimerCancelled { name: name.to_string() });
    }

    pub(crate) fn actions(&mut self) -> Vec<ProcessEvent> {
        self.actions.drain(..).collect()
    }
}
