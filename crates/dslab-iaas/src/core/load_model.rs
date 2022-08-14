//! Resource load models.

use dyn_clone::{clone_trait_object, DynClone};

/// A resource load model is a function, which defines load of resource X at the moment.
/// time - current simulation time, time_from_start - time from previous initialization
/// which allows to model load peak at the beginning of VM lifecycle.
/// This time is dropped to zero when VM is migrated.
pub trait LoadModel: DynClone {
    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
}

clone_trait_object!(LoadModel);

/// The simplest load model, the constant load.
#[derive(Clone)]
pub struct ConstLoadModel {
    load: f64,
}

impl ConstLoadModel {
    pub fn new(load: f64) -> Self {
        Self { load }
    }
}

impl LoadModel for ConstLoadModel {
    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        self.load
    }
}
