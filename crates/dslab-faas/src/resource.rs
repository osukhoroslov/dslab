use std::collections::hash_map::Iter;
use std::collections::HashMap;

#[derive(Default)]
pub struct ResourceNameResolver {
    map: HashMap<String, usize>,
}

impl ResourceNameResolver {
    pub fn try_resolve(&self, name: &str) -> Option<usize> {
        self.map.get(name).copied()
    }

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

#[derive(Clone)]
pub struct Resource {
    id: usize,
    available: u64,
    consumed: u64,
}

impl Resource {
    pub fn new(id: usize, available: u64) -> Self {
        Self {
            id,
            available,
            consumed: 0,
        }
    }

    pub fn can_allocate(&self, req: &ResourceRequirement) -> bool {
        if req.quantity + self.consumed <= self.available {
            return true;
        }
        false
    }

    pub fn allocate(&mut self, req: &ResourceRequirement) {
        self.consumed += req.quantity;
    }

    pub fn release(&mut self, req: &ResourceRequirement) {
        self.consumed -= req.quantity;
    }

    pub fn get_available(&self) -> u64 {
        self.available
    }
}

#[derive(Clone)]
pub struct ResourceRequirement {
    pub id: usize,
    pub quantity: u64,
}

impl ResourceRequirement {
    pub fn new(id: usize, quantity: u64) -> Self {
        Self { id, quantity }
    }
}

#[derive(Clone, Default)]
pub struct ResourceProvider {
    resources: HashMap<usize, Resource>,
}

impl ResourceProvider {
    pub fn new(mut resources: Vec<Resource>) -> Self {
        let mut map = HashMap::new();
        for r in resources.drain(..) {
            map.insert(r.id, r);
        }
        Self { resources: map }
    }

    pub fn new_empty() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

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

    pub fn allocate(&mut self, consumer: &ResourceConsumer) {
        for (id, req) in consumer.iter() {
            self.resources.get_mut(id).unwrap().allocate(req);
        }
    }

    pub fn release(&mut self, consumer: &ResourceConsumer) {
        for (id, req) in consumer.iter() {
            self.resources.get_mut(id).unwrap().release(req);
        }
    }

    pub fn get_resource(&self, id: usize) -> Option<&Resource> {
        self.resources.get(&id)
    }
}

#[derive(Clone, Default)]
pub struct ResourceConsumer {
    resources: HashMap<usize, ResourceRequirement>,
}

impl ResourceConsumer {
    pub fn new(mut resources: Vec<ResourceRequirement>) -> Self {
        let mut map = HashMap::new();
        for r in resources.drain(..) {
            map.insert(r.id, r);
        }
        Self { resources: map }
    }

    pub fn new_empty() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    pub fn iter(&self) -> Iter<usize, ResourceRequirement> {
        self.resources.iter()
    }
}
