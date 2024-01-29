use std::collections::HashMap;

use super::shared_state::{AwaitKey, EventPromise};
use crate::Id;

#[derive(Clone)]
pub struct EventPromisesStorage {
    promises: HashMap<AwaitKey, HashMap<Id, EventPromise>>,
}

impl EventPromisesStorage {
    pub fn new() -> Self {
        Self {
            promises: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: AwaitKey, source: Id, awaiter: EventPromise) -> Option<EventPromise> {
        self.promises.entry(key).or_default().insert(source, awaiter)
    }

    pub fn remove(&mut self, key: &AwaitKey, source: &Id) -> Option<EventPromise> {
        if let Some(promises) = self.promises.get_mut(key) {
            return promises.remove(source);
        }
        None
    }

    pub fn has_any_promise_on_key(&self, key: &AwaitKey) -> Option<Id> {
        if let Some(promises) = self.promises.get(key) {
            if !promises.is_empty() {
                return Some(*promises.keys().next().unwrap());
            }
        }
        None
    }

    pub fn has_promise_on_key(&self, key: &AwaitKey, src: &Id) -> bool {
        if let Some(promises) = self.promises.get(key) {
            return promises.contains_key(src);
        }
        false
    }

    pub fn remove_component_promises(&mut self, component_id: Id) {
        self.promises.retain(|key, _| key.to != component_id);
    }
}
