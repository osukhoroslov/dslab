use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::Id;

use super::shared_state::{AwaitKey, AwaitResultSetter};

#[derive(Clone)]
pub struct Awaiter {
    pub(crate) state: Rc<RefCell<dyn AwaitResultSetter>>,
}

impl Awaiter {
    pub fn is_shared(&self) -> bool {
        Rc::strong_count(&self.state) > 1
    }
}

#[derive(Clone)]
pub struct AwaitersWithSourceStorage {
    awaiters: HashMap<AwaitKey, HashMap<Id, Awaiter>>,
    total_size: usize,
}

impl AwaitersWithSourceStorage {
    pub fn new() -> Self {
        Self {
            awaiters: HashMap::new(),
            total_size: 0,
        }
    }

    pub fn insert(&mut self, key: AwaitKey, source: Id, awaiter: Awaiter) -> Option<Awaiter> {
        if let Some(awaiter) = self.awaiters.entry(key).or_default().insert(source, awaiter) {
            Some(awaiter)
        } else {
            self.total_size += 1;
            None
        }
    }

    pub fn remove(&mut self, key: &AwaitKey, source: &Id) -> Option<Awaiter> {
        if let Some(awaiters) = self.awaiters.get_mut(key) {
            if let Some(awaiter) = awaiters.remove(source) {
                self.total_size -= 1;
                return Some(awaiter);
            }
        }
        None
    }

    pub fn has_any_shared_awaiter_on_key(&mut self, key: &AwaitKey) -> Option<Id> {
        if let Some(awaiters) = self.awaiters.get_mut(key) {
            let mut result = None;
            let mut keys_to_remove = Vec::new();
            for (source, awaiter) in awaiters.iter() {
                if awaiter.is_shared() {
                    result = Some(*source);
                    break;
                } else {
                    keys_to_remove.push(*source);
                }
            }
            for key in keys_to_remove {
                awaiters.remove(&key);
                self.total_size -= 1;
            }
            return result;
        }
        None
    }

    pub fn has_shared_awaiter_on_key_with_source(&mut self, key: &AwaitKey, src: &Id) -> bool {
        if let Some(awaiters) = self.awaiters.get_mut(key) {
            if let Some(awaiter) = awaiters.get(src) {
                if awaiter.is_shared() {
                    return true;
                } else {
                    awaiters.remove(src);
                    self.total_size -= 1;
                }
            }
        }
        false
    }

    pub fn remove_not_shared_awaiters(&mut self) {
        self.awaiters.retain(|_key, awaiters| {
            awaiters.retain(|_source, awaiter| {
                if awaiter.is_shared() {
                    true
                } else {
                    self.total_size -= 1;
                    false
                }
            });
            !awaiters.is_empty()
        });
    }

    pub fn len(&self) -> usize {
        self.total_size
    }
}
