use std::{
    borrow::Borrow,
    cell::{Ref, RefCell},
    collections::{BinaryHeap, HashMap, HashSet},
    rc::Rc,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
};

use dslab_core::{
    event::{EventData, EventId},
    Event, Id,
};
use futures::Future;
use rand::{
    distributions::{
        uniform::{SampleRange, SampleUniform},
        Alphanumeric, DistString,
    },
    prelude::Distribution,
    Rng, SeedableRng,
};
use rand_pcg::Pcg64;

use crate::{
    executor::Executor,
    log::log_incorrect_event,
    shared_state::{AwaitKey, EmptyData, EventFuture, EventSetter, SharedState, TimerFuture},
    task::Task,
    timer::Timer,
};

pub struct AsyncSimulationState {
    clock: f64,
    rand: Pcg64,
    events: BinaryHeap<Event>,
    canceled_events: HashSet<EventId>,
    event_count: u64,

    awaiters: HashMap<AwaitKey, Rc<RefCell<dyn EventSetter>>>,
    timers: BinaryHeap<Timer>,

    task_sender: SyncSender<Arc<Task>>,
}

impl AsyncSimulationState {
    pub fn new(seed: u64, task_sender: SyncSender<Arc<Task>>) -> Self {
        Self {
            clock: 0.0,
            rand: Pcg64::seed_from_u64(seed),
            events: BinaryHeap::new(),
            canceled_events: HashSet::new(),
            event_count: 0,
            awaiters: HashMap::new(),
            timers: BinaryHeap::new(),

            task_sender,
        }
    }

    pub fn time(&self) -> f64 {
        self.clock
    }

    pub fn set_time(&mut self, time: f64) {
        self.clock = time;
    }

    pub fn rand(&mut self) -> f64 {
        self.rand.gen_range(0.0..1.0)
    }

    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.rand.gen_range(range)
    }

    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&mut self, dist: &Dist) -> T {
        dist.sample(&mut self.rand)
    }

    pub fn random_string(&mut self, len: usize) -> String {
        Alphanumeric.sample_string(&mut self.rand, len)
    }

    pub fn add_event<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            time: self.clock + delay,
            src,
            dest,
            data: Box::new(data),
        };
        if delay >= 0. {
            self.events.push(event);
            self.event_count += 1;
            event_id
        } else {
            log_incorrect_event(event, &format!("negative delay {}", delay));
            panic!("Event delay is negative! It is not allowed to add events from the past.");
        }
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    pub fn cancel_event(&mut self, id: EventId) {
        self.canceled_events.insert(id);
    }

    pub fn cancel_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        for event in self.events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
    }

    pub fn peek_event(&self) -> Option<&Event> {
        self.events.peek()
    }

    pub fn peek_timer(&self) -> Option<&Timer> {
        self.timers.peek()
    }

    pub fn next_event(&mut self) -> Option<Event> {
        if let Some(event) = self.events.pop() {
            if !self.canceled_events.remove(&event.id) {
                self.clock = event.time;
                return Some(event);
            }
        }
        return None;
    }

    pub fn next_timer(&mut self) -> Option<Timer> {
        if let Some(timer) = self.timers.pop() {
            self.clock = timer.time;
            return Some(timer);
        }
        return None;
    }

    pub fn has_handler_on_key(&self, key: &AwaitKey) -> bool {
        self.awaiters.contains_key(key)
    }

    pub fn set_event_for_await_key(&mut self, key: &AwaitKey, event: Event) -> bool {
        if !self.awaiters.contains_key(key) {
            return false;
        }

        let shared_state = self.awaiters.remove(key).unwrap();

        shared_state.borrow_mut().set_ok_completed_with_event(event);

        return true;
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        let task = Arc::new(Task::new(future, self.task_sender.clone()));

        self.task_sender.send(task).expect("too many tasks queued");
    }

    pub fn wait_for(&mut self, timeout: f64) -> TimerFuture {
        let state = Rc::new(RefCell::new(SharedState::<EmptyData>::default()));

        self.timers.push(Timer::new(self.time() + timeout, state.clone()));

        TimerFuture { state }
    }

    pub fn add_timer_on_state(&mut self, timeout: f64, state: Rc<RefCell<dyn EventSetter>>) {
        self.timers.push(Timer::new(self.time() + timeout, state.clone()));
    }

    pub fn add_awaiter_handler(&mut self, key: AwaitKey, state: Rc<RefCell<dyn EventSetter>>) {
        self.awaiters.insert(key, state);
    }
}
