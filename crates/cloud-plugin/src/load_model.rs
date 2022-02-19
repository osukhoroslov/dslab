use std::fmt;

use dyn_clone::DynClone;
use std::fmt::Debug;

pub trait LoadModel: DynClone {
    fn init(&mut self);
    fn get_resource_load(&self, sim_timestamp: f64, vm_age: f64) -> f64;
}

impl fmt::Debug for dyn LoadModel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("").finish()
    }
}

dyn_clone::clone_trait_object!(LoadModel);

#[derive(Clone, Debug)]
pub struct DefaultLoadModel;

impl DefaultLoadModel {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadModel for DefaultLoadModel {
    fn init(&mut self) {}

    fn get_resource_load(&self, _sim_timestamp: f64, _vm_age: f64) -> f64 {
        1.
    }
}
