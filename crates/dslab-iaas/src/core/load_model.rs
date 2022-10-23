//! Resource load models.

use dyn_clone::{clone_trait_object, DynClone};
use strum_macros::EnumString;

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

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum LoadModelType {
    Const,
}

pub fn parse_load_model(raw_data: String) -> Box<dyn LoadModel> {
    let cleanup = raw_data.replace("]", "").replace("\"", "");
    let split = cleanup.split("[").collect::<Vec<&str>>();
    let model_type: LoadModelType = split.get(0).unwrap().parse().unwrap();
    let model_args = split.get(1).unwrap().to_string();

    match model_type {
        LoadModelType::Const => {
            let mut result = ConstantLoadModel::new_fwd();
            result.parse_config_args(model_args);
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
    /// Parse config string e.g. "load=0.5" leads to self.load = 0.5
    fn parse_config_args(&mut self, config_str: String) {
        let variables = config_str.split(",");
        for variable in variables {
            let split = variable.split("=").collect::<Vec<&str>>();
            if split.get(0).unwrap().to_string() == "load" {
                self.load = split.get(1).unwrap().parse::<f64>().unwrap();
            }
        }
    }

    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        self.load
    }
}
