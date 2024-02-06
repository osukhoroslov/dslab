use std::{any::TypeId, collections::HashMap};

use super::{event_future::EventPromise, EventKey};
use crate::{event::EventData, Event, Id};

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

    pub fn insert<T: EventData>(
        &mut self,
        dst: Id,
        event_key: Option<EventKey>,
        src_opt: Option<Id>,
        promise: EventPromise,
    ) {
        let key = AwaitKey::new::<T>(dst, event_key);
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

    pub fn remove<T: EventData>(
        &mut self,
        dst: Id,
        event_key: Option<EventKey>,
        src_opt: &Option<Id>,
    ) -> Option<EventPromise> {
        let key = AwaitKey::new::<T>(dst, event_key);
        if let Some(source) = src_opt {
            if let Some(promises) = self.promises_with_source.get_mut(&key) {
                promises.remove(source)
            } else {
                None
            }
        } else {
            self.promises.remove(&key)
        }
    }

    pub fn has_promise_for(&self, event: &Event, event_key: Option<EventKey>) -> bool {
        let key = AwaitKey::new_by_ref(event.dst, event.data.as_ref(), event_key);
        if self.promises.contains_key(&key) {
            return true;
        }
        if let Some(promises) = self.promises_with_source.get(&key) {
            return promises.contains_key(&event.src);
        }
        false
    }

    pub fn extract_promise_for(&mut self, event: &Event, event_key: Option<EventKey>) -> Option<EventPromise> {
        let key = AwaitKey::new_by_ref(event.dst, event.data.as_ref(), event_key);
        if let Some(promise) = self.promises.remove(&key) {
            return Some(promise);
        }
        if let Some(promises) = self.promises_with_source.get_mut(&key) {
            return promises.remove(&event.src);
        }
        None
    }

    pub fn remove_component_promises(&mut self, component_id: Id) -> u32 {
        let mut removed_count = 0;
        self.promises_with_source.retain(|key, promises| {
            if key.to == component_id {
                promises.iter_mut().for_each(|(_, promise)| {
                    promise.drop_shared_state();
                    removed_count += 1;
                });
                return false;
            }

            true
        });
        self.promises.retain(|key, promise| {
            if key.to == component_id {
                promise.drop_shared_state();
                removed_count += 1;
                return false;
            }
            true
        });

        removed_count
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

#[derive(Hash, PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) struct AwaitKey {
    pub to: Id,
    pub msg_type: TypeId,
    event_key: Option<EventKey>,
}

impl AwaitKey {
    pub fn new<T: EventData>(to: Id, event_key: Option<EventKey>) -> Self {
        Self {
            to,
            msg_type: TypeId::of::<T>(),
            event_key,
        }
    }

    pub fn new_by_ref(to: Id, data: &dyn EventData, event_key: Option<EventKey>) -> Self {
        Self {
            to,
            msg_type: data.type_id(),
            event_key,
        }
    }
}
