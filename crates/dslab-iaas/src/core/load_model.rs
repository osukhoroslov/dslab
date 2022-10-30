//! Resource load models.

use std::num::ParseIntError;
use std::str::FromStr;

use dyn_clone::{clone_trait_object, DynClone};
use strum_macros::EnumString;

use crate::core::config::parse_model_name_and_args;
use crate::core::config::parse_options;

/// A resource load model is a function, which defines load of resource X at the moment.
/// time - current simulation time, time_from_start - time from previous initialization
/// which allows to model load peak at the beginning of VM lifecycle.
/// This time is dropped to zero when VM is migrated.
pub trait LoadModel: DynClone {
    fn get_resource_load(&self, time: f64, time_from_start: f64) -> f64;
}

clone_trait_object!(LoadModel);

#[derive(Clone, Debug, PartialEq, EnumString)]
pub enum LoadModelType {
    Const,
}

pub fn parse_load_model(raw_data: String) -> Box<dyn LoadModel> {
    let (model_type_str, model_args) = parse_model_name_and_args(&raw_data);
    let model_type: LoadModelType = model_type_str.parse().unwrap();
    match model_type {
        LoadModelType::Const => Box::new(ConstantLoadModel::from_str(&model_args).unwrap()),
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
}

impl LoadModel for ConstantLoadModel {
    fn get_resource_load(&self, _time: f64, _time_from_start: f64) -> f64 {
        self.load
    }
}

impl FromStr for ConstantLoadModel {
    type Err = ParseIntError;

    fn from_str(config_str: &str) -> Result<Self, Self::Err> {
        let options = parse_options(config_str);
        let load = options.get("load").unwrap().parse::<f64>().unwrap();
        Ok(ConstantLoadModel { load })
    }
}
