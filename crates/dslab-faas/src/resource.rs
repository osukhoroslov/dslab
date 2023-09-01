//! Resource model.
//!
//! The simulator considers all resources to be renewable and all resources except CPU to be strictly isolated among containers.
use std::collections::HashMap;

use crate::util::{VecMap, VecMapIterator};

/// Transforms resource name to resource id in current simulation.
#[derive(Default)]
pub struct ResourceNameResolver {
    map: HashMap<String, usize>,
}

impl ResourceNameResolver {
    /// Transforms name to id if this resource exists.
    pub fn try_resolve(&self, name: &str) -> Option<usize> {
        self.map.get(name).copied()
    }

    /// Same as [`Self::try_resolve`], but creates new resource if it doesn't exist.
    pub fn resolve(&mut self, name: &str) -> usize {
        if let Some(id) = self.try_resolve(name) {
            id
        } else {
            let id = self.map.len();
            self.map.insert(name.to_string(), id);
            id
        }
    }
}

/// A simple strictly isolated renewable resource.
#[derive(Clone)]
pub struct Resource {
    id: usize,
    available: u64,
    consumed: u64,
}

impl Resource {
    /// Creates new resource.
    pub fn new(id: usize, available: u64) -> Self {
        Self {
            id,
            available,
            consumed: 0,
        }
    }

    /// Checks whether it is possible to allocate given amount of resource.
    pub fn can_allocate(&self, req: &ResourceRequirement) -> bool {
        if req.quantity + self.consumed <= self.available {
            return true;
        }
        false
    }

    /// Allocates free resource.
    pub fn allocate(&mut self, req: &ResourceRequirement) {
        self.consumed += req.quantity;
    }

    /// Releases allocated resource.
    pub fn release(&mut self, req: &ResourceRequirement) {
        self.consumed -= req.quantity;
    }

    /// Returns amount of free resource.
    pub fn get_available(&self) -> u64 {
        self.available
    }
}

/// Requirement for allocating specified amount of a resource.
#[derive(Clone)]
pub struct ResourceRequirement {
    /// Resource id.
    pub id: usize,
    /// Required resource quantity.
    pub quantity: u64,
}

impl ResourceRequirement {
    /// Creates new ResourceRequirement.
    pub fn new(id: usize, quantity: u64) -> Self {
        Self { id, quantity }
    }
}

/// A model of a host or other entity that provides several resources to the consumers.
#[derive(Clone, Default)]
pub struct ResourceProvider {
    resources: VecMap<Resource>,
}

impl ResourceProvider {
    /// Creates new ResourceProvider.
    pub fn new(mut resources: Vec<Resource>) -> Self {
        let mut map = VecMap::default();
        for r in resources.drain(..) {
            map.insert(r.id, r);
        }
        Self { resources: map }
    }

    /// Creates new ResourceProvider without resources.
    pub fn new_empty() -> Self {
        Self {
            resources: Default::default(),
        }
    }

    /// Checks whether it is possible to allocate resources required by the consumer.
    pub fn can_allocate(&self, consumer: &ResourceConsumer) -> bool {
        for (id, req) in consumer.iter() {
            if let Some(resource) = self.resources.get(id) {
                if !resource.can_allocate(req) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    /// Allocates resources required by the consumer.
    pub fn allocate(&mut self, consumer: &ResourceConsumer) {
        for (id, req) in consumer.iter() {
            self.resources.get_mut(id).unwrap().allocate(req);
        }
    }

    /// Releases resources allocated by the consumer.
    pub fn release(&mut self, consumer: &ResourceConsumer) {
        for (id, req) in consumer.iter() {
            self.resources.get_mut(id).unwrap().release(req);
        }
    }

    /// Returns reference to a resource specified by `id` if it exists.
    pub fn get_resource(&self, id: usize) -> Option<&Resource> {
        self.resources.get(id)
    }
}

/// A model of a container or other entity that consumes resources offered by the provider.
#[derive(Clone, Default)]
pub struct ResourceConsumer {
    resources: VecMap<ResourceRequirement>,
}

impl ResourceConsumer {
    /// Creates new ResourceConsumer.
    pub fn new(mut resources: Vec<ResourceRequirement>) -> Self {
        let mut map = VecMap::default();
        for r in resources.drain(..) {
            map.insert(r.id, r);
        }
        Self { resources: map }
    }

    /// Creates new ResourceConsumer without resources.
    pub fn new_empty() -> Self {
        Self {
            resources: Default::default(),
        }
    }

    /// Iterates over resource requirements of the consumer.
    pub fn iter(&self) -> VecMapIterator<ResourceRequirement> {
        self.resources.iter()
    }
}
