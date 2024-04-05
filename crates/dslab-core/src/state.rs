use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::component::Id;
use crate::event::{Event, EventData, EventId};
use crate::log::log_incorrect_event;
use crate::{async_mode_disabled, async_mode_enabled};

async_mode_enabled!(
    use std::any::TypeId;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc::Sender;

    use futures::Future;

    use crate::async_mode::EventKey;
    use crate::async_mode::promise_store::EventPromiseStore;
    use crate::async_mode::event_future::{EventFuture, EventPromise};
    use crate::async_mode::task::Task;
    use crate::async_mode::timer_future::{TimerPromise, TimerId, TimerFuture};
);

/// Epsilon to compare floating point values for equality.
pub const EPSILON: f64 = 1e-12;

async_mode_disabled!(
    #[derive(Clone)]
    pub struct SimulationState {
        clock: f64,
        rand: Pcg64,
        events: BinaryHeap<Event>,
        ordered_events: VecDeque<Event>,
        canceled_events: HashSet<EventId>,
        event_count: u64,

        component_name_to_id: HashMap<String, Id>,
        component_names: Vec<String>,
    }
);

async_mode_enabled!(
    type KeyGetterFn = Rc<dyn Fn(&dyn EventData) -> EventKey>;

    #[derive(Clone)]
    pub struct SimulationState {
        clock: f64,
        rand: Pcg64,
        events: BinaryHeap<Event>,
        ordered_events: VecDeque<Event>,
        canceled_events: HashSet<EventId>,
        event_count: u64,

        component_name_to_id: HashMap<String, Id>,
        component_names: Vec<String>,

        // Specific to async mode
        registered_handlers: Vec<bool>,

        event_promises: EventPromiseStore,
        key_getters: HashMap<TypeId, KeyGetterFn>,

        timers: BinaryHeap<TimerPromise>,
        canceled_timers: HashSet<TimerId>,
        timer_count: u64,

        executor: Sender<Rc<Task>>,
    }
);

impl SimulationState {
    async_mode_disabled!(
        pub fn new(seed: u64) -> Self {
            Self {
                clock: 0.0,
                rand: Pcg64::seed_from_u64(seed),
                events: BinaryHeap::new(),
                ordered_events: VecDeque::new(),
                canceled_events: HashSet::new(),
                event_count: 0,
                component_name_to_id: HashMap::new(),
                component_names: Vec::new(),
            }
        }
    );
    async_mode_enabled!(
        pub fn new(seed: u64, executor: Sender<Rc<Task>>) -> Self {
            Self {
                clock: 0.0,
                rand: Pcg64::seed_from_u64(seed),
                events: BinaryHeap::new(),
                ordered_events: VecDeque::new(),
                canceled_events: HashSet::new(),
                event_count: 0,
                component_name_to_id: HashMap::new(),
                component_names: Vec::new(),
                // Specific to async mode
                registered_handlers: Vec::new(),
                event_promises: EventPromiseStore::new(),
                key_getters: HashMap::new(),
                timers: BinaryHeap::new(),
                canceled_timers: HashSet::new(),
                timer_count: 0,
                executor,
            }
        }
    );

    pub fn register(&mut self, name: &str) -> Id {
        if let Some(&id) = self.component_name_to_id.get(name) {
            return id;
        }
        let id = self.component_name_to_id.len() as Id;
        self.component_name_to_id.insert(name.to_owned(), id);
        self.component_names.push(name.to_owned());
        self.on_register();
        id
    }

    pub fn lookup_id(&self, name: &str) -> Id {
        *self.component_name_to_id.get(name).unwrap()
    }

    pub fn lookup_name(&self, id: Id) -> String {
        self.component_names[id as usize].clone()
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

    pub fn add_event<T>(&mut self, data: T, src: Id, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            time: self.clock + delay.max(0.),
            src,
            dst,
            data: Box::new(data),
        };
        if delay >= -EPSILON {
            self.events.push(event);
            self.event_count += 1;
            event_id
        } else {
            log_incorrect_event(event, &format!("negative delay {}", delay));
            panic!("Event delay is negative! It is not allowed to add events from the past.");
        }
    }

    pub fn add_ordered_event<T>(&mut self, data: T, src: Id, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        if !self.can_add_ordered_event(delay) {
            panic!("Event order is broken! Ordered events should be added in non-decreasing order of their time.");
        }
        let last_time = self.ordered_events.back().map_or(f64::MIN, |x| x.time);
        let event_id = self.event_count;
        let event = Event {
            id: event_id,
            // max is used to enforce time order despite the floating-point errors
            time: last_time.max(self.clock + delay),
            src,
            dst,
            data: Box::new(data),
        };
        if delay >= 0. {
            self.ordered_events.push_back(event);
            self.event_count += 1;
            event_id
        } else {
            log_incorrect_event(event, &format!("negative delay {}", delay));
            panic!("Event delay is negative! It is not allowed to add events from the past.");
        }
    }

    pub fn can_add_ordered_event(&self, delay: f64) -> bool {
        if let Some(evt) = self.ordered_events.back() {
            // small epsilon is used to account for floating-point errors
            if delay + self.clock < evt.time - EPSILON {
                return false;
            }
        }
        true
    }

    pub fn next_event(&mut self) -> Option<Event> {
        loop {
            let maybe_heap = self.events.peek();
            let maybe_deque = self.ordered_events.front();
            if maybe_heap.is_some() && (maybe_deque.is_none() || maybe_heap.unwrap() > maybe_deque.unwrap()) {
                let event = self.events.pop().unwrap();
                if !self.canceled_events.remove(&event.id) {
                    self.clock = event.time;
                    return Some(event);
                }
            } else if maybe_deque.is_some() {
                let event = self.ordered_events.pop_front().unwrap();
                if !self.canceled_events.remove(&event.id) {
                    self.clock = event.time;
                    return Some(event);
                }
            } else {
                return None;
            }
        }
    }

    pub fn peek_event(&mut self) -> Option<&Event> {
        loop {
            let heap_event = self.events.peek();
            let heap_event_id = heap_event.map(|e| e.id).unwrap_or(0);
            let deque_event = self.ordered_events.front();
            let deque_event_id = deque_event.map(|e| e.id).unwrap_or(0);

            if heap_event.is_some() && (deque_event.is_none() || heap_event.unwrap() > deque_event.unwrap()) {
                if self.canceled_events.remove(&heap_event_id) {
                    self.events.pop().unwrap();
                } else {
                    return self.events.peek();
                }
            } else if deque_event.is_some() {
                if self.canceled_events.remove(&deque_event_id) {
                    self.ordered_events.pop_front().unwrap();
                } else {
                    return self.ordered_events.front();
                }
            } else {
                return None;
            }
        }
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
        for event in self.ordered_events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
    }

    pub fn cancel_and_get_events<F>(&mut self, pred: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let mut events = Vec::new();
        for event in self.events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
                events.push(event.clone());
            }
        }
        for event in self.ordered_events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
                events.push(event.clone());
            }
        }
        events
    }

    // This function does not check events from ordered_events.
    pub fn cancel_heap_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        for event in self.events.iter() {
            if pred(event) {
                self.canceled_events.insert(event.id);
            }
        }
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    pub fn dump_events(&self) -> Vec<Event> {
        let mut output = Vec::new();
        for event in self.events.iter() {
            if !self.canceled_events.contains(&event.id) {
                output.push((*event).clone())
            }
        }
        for event in self.ordered_events.iter() {
            if !self.canceled_events.contains(&event.id) {
                output.push((*event).clone())
            }
        }
        output.sort();
        // Because the sorting order of events is inverted to be used with BinaryHeap
        output.reverse();
        output
    }

    async_mode_disabled!(
        fn on_register(&mut self) {}
        pub fn on_handler_added(&mut self, _id: Id) {}
        pub fn on_handler_removed(&mut self, _id: Id) {}
    );

    async_mode_enabled!(
        // Components --------------------------------------------------------------------------------------------------

        fn on_register(&mut self) {
            self.registered_handlers.push(false)
        }

        pub fn on_handler_added(&mut self, id: Id) {
            self.registered_handlers[id as usize] = true;
        }

        pub fn on_handler_removed(&mut self, id: Id) {
            self.registered_handlers[id as usize] = false;
        }

        fn has_registered_handler(&self, id: Id) -> bool {
            self.registered_handlers
                .get(id as usize)
                .map_or_else(|| false, |flag| *flag)
        }

        // Spawning async tasks ----------------------------------------------------------------------------------------

        pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
            Task::spawn(future, self.executor.clone());
        }

        pub fn spawn_component(&mut self, component_id: Id, future: impl Future<Output = ()>) {
            assert!(
                self.has_registered_handler(component_id),
                "Spawning async tasks for component without registered event handler is not supported. \
                Register handler for component {} before spawning tasks for it (empty impl EventHandler is OK).",
                component_id,
            );
            Task::spawn(future, self.executor.clone());
        }

        // Timers ------------------------------------------------------------------------------------------------------

        pub fn create_timer(
            &mut self,
            component_id: Id,
            timeout: f64,
            sim_state: Rc<RefCell<SimulationState>>,
        ) -> TimerFuture {
            let timer_promise = TimerPromise::new(self.timer_count, component_id, self.time() + timeout);
            let timer_future = timer_promise.future(sim_state);
            self.timers.push(timer_promise);
            self.timer_count += 1;
            timer_future
        }

        pub fn peek_timer(&mut self) -> Option<&TimerPromise> {
            loop {
                let maybe_timer = self.timers.peek();
                let timer_id = maybe_timer.map(|t| t.id).unwrap_or(0);
                if maybe_timer.is_some() {
                    if self.canceled_timers.remove(&timer_id) {
                        self.timers.pop();
                    } else {
                        return self.timers.peek();
                    }
                } else {
                    return None;
                }
            }
        }

        pub fn next_timer(&mut self) -> Option<TimerPromise> {
            loop {
                if let Some(timer) = self.timers.pop() {
                    if !self.canceled_timers.remove(&timer.id) {
                        self.clock = timer.time;
                        return Some(timer);
                    }
                } else {
                    return None;
                }
            }
        }

        // Called when component handler is removed.
        pub fn cancel_component_timers(&mut self, component_id: Id) {
            let mut cancelled_count = 0;
            self.timers.retain(|timer_promise| {
                if timer_promise.component_id == component_id {
                    timer_promise.drop_state();
                    cancelled_count += 1;
                    return false;
                }
                true
            });
            if cancelled_count > 0 {
                log::warn!(
                    target: "simulation",
                    "[{:.3} {} simulation] {} active timers for component `{}` are cancelled",
                    self.time(),
                    crate::log::get_colored("WARN", colored::Color::Yellow),
                    cancelled_count,
                    self.lookup_name(component_id),
                )
            }
        }

        // Called by dropped TimerFuture that was not completed.
        pub fn on_incomplete_timer_future_drop(&mut self, timer_id: TimerId) {
            self.canceled_timers.insert(timer_id);
        }

        // Event futures and promises ----------------------------------------------------------------------------------

        pub fn create_event_future<T: EventData>(
            &mut self,
            dst: Id,
            src: Option<Id>,
            key: Option<EventKey>,
            sim_state: Rc<RefCell<SimulationState>>,
        ) -> EventFuture<T> {
            let (promise, future) = EventPromise::contract(dst, src, key, sim_state);
            self.event_promises.insert::<T>(dst, src, key, promise);
            future
        }

        pub fn has_event_promise_for(&self, event: &Event, event_key: Option<EventKey>) -> bool {
            self.event_promises.has_promise_for(event, event_key)
        }

        pub fn complete_event_promise(&mut self, event: Event, event_key: Option<EventKey>) {
            // panics if there is no promise
            let promise = self.event_promises.remove_promise_for(&event, event_key).unwrap();
            promise.complete(event);
        }

        // Called when component handler is removed.
        pub fn cancel_component_promises(&mut self, component_id: Id) {
            let cancelled_count = self.event_promises.drop_promises_by_dst(component_id);
            if cancelled_count > 0 {
                log::warn!(
                    target: "simulation",
                    "[{:.3} {} simulation] {} active evnet promises for component `{}` are cancelled",
                    self.time(),
                    crate::log::get_colored("WARN", colored::Color::Yellow),
                    cancelled_count,
                    self.lookup_name(component_id),
                )
            }
        }

        // Called by dropped EventFuture that was not completed.
        pub fn on_incomplete_event_future_drop<T: EventData>(
            &mut self,
            dst: Id,
            src: &Option<Id>,
            event_key: Option<EventKey>,
        ) {
            self.event_promises.remove::<T>(dst, src, event_key);
        }

        // Event key getters -------------------------------------------------------------------------------------------

        pub fn register_key_getter_for<T: EventData>(&mut self, key_getter: impl Fn(&T) -> EventKey + 'static) {
            self.key_getters.insert(
                TypeId::of::<T>(),
                Rc::new(move |raw_data| {
                    if let Some(data) = raw_data.downcast_ref::<T>() {
                        key_getter(data)
                    } else {
                        panic!(
                            "Key getter for type {} is incorrectly used for type {}",
                            std::any::type_name::<T>(),
                            serde_type_name::type_name(&raw_data).unwrap(),
                        );
                    }
                }),
            );
        }

        pub fn get_key_getter(&self, type_id: TypeId) -> Option<KeyGetterFn> {
            self.key_getters.get(&type_id).cloned()
        }
    );
}
