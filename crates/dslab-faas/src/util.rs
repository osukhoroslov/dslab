//! Various utility structs.
use std::hash::BuildHasherDefault;
use std::ops::{Index, IndexMut};

use indexmap::{IndexMap, IndexSet};
use rustc_hash::FxHasher;
use serde::ser::{SerializeSeq, Serializer};
use serde::Serialize;

/// A simple incrementing counter.
#[derive(Default)]
pub struct Counter {
    value: usize,
}

impl Counter {
    /// Returns current counter value.
    pub fn curr(&self) -> usize {
        self.value
    }

    /// Post-increments the counter.
    pub fn increment(&mut self) -> usize {
        let curr = self.value;
        self.value += 1;
        curr
    }
}

#[derive(Clone)]
/// A simple mapping type for storing (key, value) pairs where the keys are assumed to be integers taken
/// from some unknown but not very big interval [0, MAX_KEY]. This map does not support deletion.
pub struct VecMap<T> {
    data: Vec<Option<T>>,
}

impl<T> Default for VecMap<T> {
    fn default() -> Self {
        Self { data: Vec::new() }
    }
}

impl<T> VecMap<T> {
    /// Inserts a new value into the map.
    pub fn insert(&mut self, id: usize, value: T) {
        while self.data.len() <= id {
            self.data.push(None);
        }
        self.data[id] = Some(value);
    }

    /// Returns a reference to the value specified by id if exists.
    pub fn get(&self, id: usize) -> Option<&T> {
        if id < self.data.len() {
            self.data[id].as_ref()
        } else {
            None
        }
    }

    /// Returns a mutable reference to the value specified by id if exists.
    pub fn get_mut(&mut self, id: usize) -> Option<&mut T> {
        if id < self.data.len() {
            self.data[id].as_mut()
        } else {
            None
        }
    }

    /// Returns (key, value) pairs iterator.
    pub fn iter(&self) -> VecMapIterator<'_, T> {
        VecMapIterator::new(self.data.iter().enumerate())
    }
}

/// Iterator over [`VecMap`] (key, value) pairs.
pub struct VecMapIterator<'a, T> {
    inner: std::iter::Enumerate<std::slice::Iter<'a, Option<T>>>,
}

impl<'a, T> VecMapIterator<'a, T> {
    /// Creates new VecMapIterator.
    pub fn new(inner: std::iter::Enumerate<std::slice::Iter<'a, Option<T>>>) -> Self {
        Self { inner }
    }
}

impl<'a, T> Iterator for VecMapIterator<'a, T> {
    type Item = (usize, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        for (id, x) in self.inner.by_ref() {
            if let Some(y) = x.as_ref() {
                return Some((id, y));
            }
        }
        None
    }
}

impl<T> Index<usize> for VecMap<T> {
    type Output = T;

    fn index(&self, id: usize) -> &Self::Output {
        self.data[id].as_ref().unwrap()
    }
}

impl<T> IndexMut<usize> for VecMap<T> {
    fn index_mut(&mut self, id: usize) -> &mut Self::Output {
        self.data[id].as_mut().unwrap()
    }
}

/// Similar to [`VecMap`], but returns the default value instead of None and auto-extends to keys in
/// `get_mut` query.
#[derive(Clone, Default)]
pub struct DefaultVecMap<T: Default> {
    data: Vec<T>,
}

impl<T> DefaultVecMap<T>
where
    T: Default,
{
    fn extend(&mut self, id: usize) {
        while self.data.len() <= id {
            self.data.push(Default::default());
        }
    }

    /// Inserts a new value into the map.
    pub fn insert(&mut self, id: usize, value: T) {
        self.extend(id);
        self.data[id] = value;
    }

    /// Returns a reference to the value speficied by id if exists.
    pub fn get(&self, id: usize) -> Option<&T> {
        if id < self.data.len() {
            Some(&self.data[id])
        } else {
            None
        }
    }

    /// Returns a mutable reference to the value speficied by id if exists.
    pub fn get_mut(&mut self, id: usize) -> &mut T {
        self.extend(id);
        &mut self.data[id]
    }

    /// Returns an iterator over map values. To iterate over (key, value) pairs call enumerate() on the iterator.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.data.iter()
    }
}

impl<T> Index<usize> for DefaultVecMap<T>
where
    T: Default,
{
    type Output = T;

    fn index(&self, id: usize) -> &Self::Output {
        &self.data[id]
    }
}

impl<T> IndexMut<usize> for DefaultVecMap<T>
where
    T: Default,
{
    fn index_mut(&mut self, id: usize) -> &mut Self::Output {
        &mut self.data[id]
    }
}

impl<T> Serialize for DefaultVecMap<T>
where
    T: Default + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for x in self.data.iter() {
            seq.serialize_element(x)?;
        }
        seq.end()
    }
}

/// IndexMap with faster hash function.
pub type FxIndexMap<K, V> = IndexMap<K, V, BuildHasherDefault<FxHasher>>;
/// IndexSet with faster hash function.
pub type FxIndexSet<K> = IndexSet<K, BuildHasherDefault<FxHasher>>;
