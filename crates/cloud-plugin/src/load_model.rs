use std::fmt;

use dyn_clone::DynClone;
use std::fmt::Debug;

pub trait LoadModel: DynClone {
    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
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
    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        1.
    }
}
