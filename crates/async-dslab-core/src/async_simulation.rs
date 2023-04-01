use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
};

use dslab_core::{Event, Id};
use futures::Future;
use log::debug;
use rand::{
    distributions::uniform::{SampleRange, SampleUniform},
    prelude::Distribution,
};
use serde_json::json;

use crate::{
    async_context::AsyncSimulationContext, async_state::AsyncSimulationState, executor::Executor,
    shared_state::AwaitKey, task::Task,
};

pub struct AsyncSimulation {
    sim_state: Rc<RefCell<AsyncSimulationState>>,
    name_to_id: HashMap<String, Id>,
    names: Rc<RefCell<Vec<String>>>,

    executor: Executor,
}

impl AsyncSimulation {
    pub fn new(seed: u64) -> Self {
        const MAX_QUEUED_TASKS: usize = 10_000;
        let (task_sender, ready_queue) = sync_channel(MAX_QUEUED_TASKS);

        Self {
            sim_state: Rc::new(RefCell::new(AsyncSimulationState::new(seed, task_sender))),
            name_to_id: HashMap::new(),
            names: Rc::new(RefCell::new(Vec::new())),

            executor: Executor { ready_queue },
        }
    }

    fn register(&mut self, name: &str) -> Id {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }
        let id = self.name_to_id.len() as Id;
        self.name_to_id.insert(name.to_owned(), id);
        self.names.borrow_mut().push(name.to_owned());
        id
    }

    pub fn lookup_id(&self, name: &str) -> Id {
        *self.name_to_id.get(name).unwrap()
    }

    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }

    pub fn create_context<S>(&mut self, name: S) -> AsyncSimulationContext
    where
        S: AsRef<str>,
    {
        let ctx = AsyncSimulationContext::new(
            self.register(name.as_ref()),
            name.as_ref(),
            self.sim_state.clone(),
            self.names.clone(),
        );
        debug!(
            target: "simulation",
            "[{:.3} {} simulation] Created context: {}",
            self.time(),
            dslab_core::log::get_colored("DEBUG", colored::Color::Blue),
            json!({"name": ctx.name(), "id": ctx.id()})
        );
        ctx
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn step(&mut self) -> bool {
        self.process_tasks();

        let mut sim_state = self.sim_state.borrow_mut();
        let next_timer = sim_state.peek_timer();
        let next_event = sim_state.peek_event();

        match (next_timer, next_event) {
            (None, None) => false,
            (_, None) => {
                drop(sim_state);
                self.process_timer();
                true
            }
            (None, _) => {
                drop(sim_state);
                self.process_event();
                true
            }
            _ => {
                if next_timer.unwrap().time <= next_event.unwrap().time {
                    drop(sim_state);
                    self.process_timer();
                } else {
                    drop(sim_state);
                    self.process_event();
                }
                true
            }
        }
    }

    fn process_timer(&mut self) {
        let mut next_timer = self.sim_state.borrow_mut().next_timer().unwrap();

        next_timer.state.as_ref().borrow_mut().set_completed();

        self.process_tasks();
    }

    fn process_event(&mut self) {
        let mut next_event = self.sim_state.borrow_mut().next_event().unwrap();

        let await_key = AwaitKey {
            from: next_event.src,
            to: next_event.dest,
            msg_type: next_event.data.as_ref().type_id(),
        };

        if self.sim_state.borrow().has_handler_on_key(&await_key) {
            self.sim_state
                .borrow_mut()
                .set_event_for_await_key(&await_key, next_event);
        } else {
            panic!("kek lol mem");
        }

        self.process_tasks();
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        for _ in 0..step_count {
            if !self.step() {
                return false;
            }
        }
        true
    }

    pub fn step_until_no_events(&mut self) {
        while self.step() {}
    }

    pub fn step_for_duration(&mut self, duration: f64) -> bool {
        let end_time = self.sim_state.borrow().time() + duration;
        self.step_until_time(end_time)
    }

    pub fn step_until_time(&mut self, time: f64) -> bool {
        let mut result = true;
        loop {
            if let Some(event) = self.sim_state.borrow().peek_event() {
                if event.time > time {
                    break;
                }
            } else {
                result = false;
                break;
            }
            self.step();
        }
        self.sim_state.borrow_mut().set_time(time);
        result
    }

    pub fn rand(&mut self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.sim_state.borrow_mut().gen_range(range)
    }

    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&mut self, dist: &Dist) -> T {
        self.sim_state.borrow_mut().sample_from_distribution(dist)
    }

    pub fn random_string(&mut self, len: usize) -> String {
        self.sim_state.borrow_mut().random_string(len)
    }

    pub fn event_count(&self) -> u64 {
        self.sim_state.borrow().event_count()
    }

    pub fn cancel_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        self.sim_state.borrow_mut().cancel_events(pred);
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.sim_state.borrow_mut().spawn(future);
    }

    fn process_tasks(&self) {
        self.executor.process_tasks();
    }
}
