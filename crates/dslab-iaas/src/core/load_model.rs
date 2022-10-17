//! Resource load models.

use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

/// A resource load model is a function, which defines load of resource X at the moment.
/// time - current simulation time, time_from_start - time from previous initialization
/// which allows to model load peak at the beginning of VM lifecycle.
/// This time is dropped to zero when VM is migrated.
pub trait LoadModel: DynClone {
    /// parse load model arguments from .yaml config string
    fn parse_config_args(&mut self, config_string: String);

    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
}

clone_trait_object!(LoadModel);

/// Load model names enum.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum LoadModelType {
    Const,
}

pub fn parse_load_model(model_type: LoadModelType, args: String) -> Box<dyn LoadModel> {
    match model_type {
        LoadModelType::Const => {
            let mut result = ConstantLoadModel::new_fwd();
            result.parse_config_args(args);
            Box::new(result)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// The simplest load model, the constant load.
#[derive(Clone)]
pub struct ConstantLoadModel {
    load: f64,
}

impl ConstantLoadModel {
    pub fn new(load: f64) -> Self {
        Self { load }
    }

    pub fn new_fwd() -> Self {
        Self { load: 0. }
    }
}

impl LoadModel for ConstantLoadModel {
    fn parse_config_args(&mut self, config_str: String) {
        self.load = config_str.parse::<f64>().unwrap();
    }

    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        self.load
    }
}
