use std::collections::HashMap;

#[derive(Clone)]
pub struct Resource {
    name: String,
    available: u64,
    consumed: u64,
}

impl Resource {
    pub fn new(name: String, available: u64) -> Self {
        Self {
            name,
            available,
            consumed: 0,
        }
    }

    pub fn can_acquire(&self, req: &ResourceRequirement) -> bool {
        if req.quantity + self.consumed <= self.available {
            return true;
        }
        false
    }

    pub fn acquire(&mut self, req: &ResourceRequirement) {
        self.consumed += req.quantity;
    }

    pub fn release(&mut self, req: &ResourceRequirement) {
        self.consumed -= req.quantity;
    }
}

#[derive(Clone)]
pub struct ResourceRequirement {
    pub name: String,
    pub quantity: u64,
}

impl ResourceRequirement {
    pub fn new(name: String, quantity: u64) -> Self {
        Self { name, quantity }
    }
}

#[derive(Clone, Default)]
pub struct ResourceProvider {
    resources: HashMap<String, Resource>,
}

impl ResourceProvider {
    pub fn new(resources: HashMap<String, Resource>) -> Self {
        Self { resources }
    }

    pub fn new_empty() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    pub fn can_acquire(&self, consumer: &ResourceConsumer) -> bool {
        for (name, req) in consumer.iter() {
            if let Some(resource) = self.resources.get(name) {
                if !resource.can_acquire(req) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    pub fn acquire(&mut self, consumer: &ResourceConsumer) {
        for (name, req) in consumer.iter() {
            self.resources.get_mut(name).unwrap().acquire(req);
        }
    }

    pub fn release(&mut self, consumer: &ResourceConsumer) {
        for (name, req) in consumer.iter() {
            self.resources.get_mut(name).unwrap().release(req);
        }
    }
}

#[derive(Clone, Default)]
pub struct ResourceConsumer {
    resources: HashMap<String, ResourceRequirement>,
}

impl ResourceConsumer {
    pub fn new(resources: HashMap<String, ResourceRequirement>) -> Self {
        Self { resources }
    }

    pub fn new_empty() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    pub fn iter(&self) -> ResourceRequirementIterator<'_> {
        ResourceRequirementIterator {
            inner: self.resources.iter(),
        }
    }
}

pub struct ResourceRequirementIterator<'a> {
    inner: std::collections::hash_map::Iter<'a, String, ResourceRequirement>,
}

impl<'a> Iterator for ResourceRequirementIterator<'a> {
    type Item = (&'a String, &'a ResourceRequirement);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
