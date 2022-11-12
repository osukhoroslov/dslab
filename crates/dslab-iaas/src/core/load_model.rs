//! Resource load models.

use dyn_clone::{clone_trait_object, DynClone};

use crate::core::config::parse_config_value;
use crate::core::config::parse_options;

/// A resource load model is a function, which defines load of resource X at the moment.
/// time - current simulation time, time_from_start - time from previous initialization
/// which allows to model load peak at the beginning of VM lifecycle.
/// This time is dropped to zero when VM is migrated.
pub trait LoadModel: DynClone {
    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
}

clone_trait_object!(LoadModel);

pub fn load_model_resolver(config_str: String) -> Box<dyn LoadModel> {
    let (model_name, options) = parse_config_value(&config_str);
    match model_name.as_str() {
        "Const" => Box::new(ConstantLoadModel::from_str(&options.unwrap())),
        _ => panic!("Can't resolve: {}", config_str),
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

    fn from_str(s: &str) -> Self {
        let options = parse_options(s);
        let load = options.get("load").unwrap().parse::<f64>().unwrap();
        Self { load }
    }
}

impl LoadModel for ConstantLoadModel {
    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        self.load
    }
}
