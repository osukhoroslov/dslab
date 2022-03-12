use crate::resource::ResourceConsumer;

use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct Stats {
    pub invocations: u64,
    pub cold_starts: u64,
    pub cold_starts_total_time: f64,
    pub wasted_resource_time: HashMap<String, f64>,
}

impl Stats {
    pub fn update_wasted_resources(&mut self, time: f64, resource: &ResourceConsumer) {
        for (_, req) in resource.iter() {
            let delta = time * (req.quantity as f64);
            if let Some(old) = self.wasted_resource_time.get_mut(&req.name) {
                *old += delta;
            } else {
                self.wasted_resource_time.insert(req.name.clone(), delta);
            }
        }
    }
}
