use std::{cell::RefCell, rc::Rc};

use dslab_core::{
    event::{EventData, EventId},
    Id,
};
use futures::{stream::FuturesUnordered, Future};

use crate::{
    async_state::AsyncSimulationState,
    shared_state::{AwaitKey, AwaitResult, EventFuture, SharedState, TimerFuture},
};

pub struct AsyncSimulationContext {
    id: Id,
    name: String,
    sim_state: Rc<RefCell<AsyncSimulationState>>,
    names: Rc<RefCell<Vec<String>>>,
}

impl AsyncSimulationContext {
    pub(crate) fn new(
        id: Id,
        name: &str,
        sim_state: Rc<RefCell<AsyncSimulationState>>,
        names: Rc<RefCell<Vec<String>>>,
    ) -> Self {
        Self {
            id,
            name: name.to_owned(),
            sim_state,
            names,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn emit<T>(&mut self, data: T, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dest, delay)
    }

    pub fn async_wait_for(&mut self, timeout: f64) -> TimerFuture {
        self.sim_state.borrow_mut().wait_for(timeout)
    }

    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) {
        self.sim_state.borrow_mut().spawn(future);
    }

    pub fn wait_for_event<T>(&mut self, src: Id, dst: Id, timeout: f64) -> EventFuture
    where
        T: EventData,
    {
        let await_key = AwaitKey {
            from: src,
            to: dst,
            msg_type: std::any::TypeId::of::<T>(),
        };

        let state = Rc::new(RefCell::new(SharedState::default()));

        self.spawn(inner_wait(self.sim_state.clone(), await_key, timeout, state.clone()));

        EventFuture { state }
    }
}

async fn inner_wait(
    simulation_state: Rc<RefCell<AsyncSimulationState>>,
    key: AwaitKey,
    timeout: f64,
    state: Rc<RefCell<SharedState>>,
) {
    state.borrow_mut().shared_content = AwaitResult::timeout_with(key.from, key.to);

    simulation_state.borrow_mut().add_awaiter_handler(key, state.clone());

    simulation_state.borrow_mut().wait_on_state(timeout, state).await;
}
