use std::collections::HashMap;

use super::shared_state::{AwaitKey, EventPromise};
use crate::Id;

#[derive(Clone)]
pub struct EventPromisesStorage {
    promises: HashMap<AwaitKey, EventPromise>,
    promises_with_source: HashMap<AwaitKey, HashMap<Id, EventPromise>>,
}

impl EventPromisesStorage {
    pub fn new() -> Self {
        Self {
            promises: HashMap::new(),
            promises_with_source: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: AwaitKey, src_opt: Option<Id>, promise: EventPromise) {
        if let Some(src) = src_opt {
            if self.promises.contains_key(&key) {
                panic!("Async event handler for key {:?} (without source) already exists", key);
            }
            if self
                .promises_with_source
                .entry(key)
                .or_default()
                .insert(src, promise)
                .is_some()
            {
                panic!(
                    "Async event handler for key {:?} and source {} already exists",
                    key, src
                );
            }
        } else {
            if let Some(src) = self.has_any_promise_on_key(&key) {
                panic!(
                    "Async event handler for key {:?} with source {} already exists",
                    key, src
                );
            }
            if self.promises.insert(key, promise).is_some() {
                panic!("Async event handler for key {:?} (without source) already exists", key);
            }
        }
    }

    pub fn remove(&mut self, key: &AwaitKey, src_opt: &Option<Id>) -> Option<EventPromise> {
        if let Some(source) = src_opt {
            if let Some(promises) = self.promises_with_source.get_mut(key) {
                promises.remove(source)
            } else {
                None
            }
        } else {
            self.promises.remove(key)
        }
    }

    pub fn has_promise_on_key(&self, key: &AwaitKey, src: &Id) -> bool {
        if self.promises.contains_key(key) {
            return true;
        }
        if let Some(promises) = self.promises_with_source.get(key) {
            return promises.contains_key(src);
        }
        false
    }

    pub fn extract_promise(&mut self, key: &AwaitKey, src: &Id) -> Option<EventPromise> {
        if let Some(promise) = self.promises.remove(key) {
            return Some(promise);
        }
        if let Some(promises) = self.promises_with_source.get_mut(key) {
            return promises.remove(src);
        }
        None
    }

    pub fn remove_component_promises(&mut self, component_id: Id) {
        self.promises_with_source.retain(|key, promises| {
            if key.to == component_id {
                promises.iter_mut().for_each(|(_, promise)| {
                    promise.drop_shared_state();
                });
                return false;
            }

            true
        });
        self.promises.retain(|key, promise| {
            if key.to == component_id {
                promise.drop_shared_state();
                return false;
            }
            true
        });
    }

    fn has_any_promise_on_key(&self, key: &AwaitKey) -> Option<Id> {
        if let Some(promises) = self.promises_with_source.get(key) {
            if !promises.is_empty() {
                return Some(*promises.keys().next().unwrap());
            }
        }
        None
    }
}
