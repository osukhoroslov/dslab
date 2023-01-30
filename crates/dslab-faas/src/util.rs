use std::hash::BuildHasherDefault;

use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;

#[derive(Default)]
pub struct Counter {
    value: u64,
}

impl Counter {
    pub fn curr(&self) -> u64 {
        self.value
    }

    pub fn increment(&mut self) -> u64 {
        let curr = self.value;
        self.value += 1;
        curr
    }
}

#[derive(Clone)]
/// A simple mapping type for storing (key, value) pairs where the keys are assumed to be taken
/// from some unknown but not very big interval [0, MAX_KEY].
pub struct VecMap<T> {
    data: Vec<Option<T>>,
}

impl<T> Default for VecMap<T> {
    fn default() -> Self {
        Self { data: Vec::new() }
    }
}

impl<T> VecMap<T> {
    pub fn insert(&mut self, id: usize, value: T) {
        while self.data.len() <= id {
            self.data.push(None);
        }
        self.data[id] = Some(value);
    }

    pub fn get(&self, id: usize) -> Option<&T> {
        if id < self.data.len() {
            self.data[id].as_ref()
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, id: usize) -> Option<&mut T> {
        if id < self.data.len() {
            self.data[id].as_mut()
        } else {
            None
        }
    }
}

/// Similar to VecMap, but returns default value instead of None and auto-extends to keys in
/// `get_mut` query.
#[derive(Clone, Default)]
pub struct RawVecMap<T: Default> {
    data: Vec<T>,
}

impl<T> RawVecMap<T>
where
    T: Default,
{
    fn extend(&mut self, id: usize) {
        while self.data.len() <= id {
            self.data.push(Default::default());
        }
    }

    pub fn insert(&mut self, id: usize, value: T) {
        self.extend(id);
        self.data[id] = value;
    }

    pub fn get(&self, id: usize) -> Option<&T> {
        if id < self.data.len() {
            Some(&self.data[id])
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, id: usize) -> &mut T {
        self.extend(id);
        &mut self.data[id]
    }
}

pub type FxIndexMap<K, V> = IndexMap<K, V, BuildHasherDefault<FxHasher>>;
pub type FxIndexSet<K> = IndexSet<K, BuildHasherDefault<FxHasher>>;
