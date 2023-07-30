//! Dynamic variables implementation.

use std::fmt::Debug;
use std::str::FromStr;

use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum NumericParam<T> {
    Value(T),
    Range { from: T, to: T, step: T },
    Values(Vec<T>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum CustomParam<T> {
    Value(T),
    Values(Vec<T>),
}

pub trait DynamicVariableTrait: Debug + DynClone {
    /// Increment config variable
    fn next(&mut self);

    /// Reset this variable to initial value
    fn reset(&mut self);

    /// Returns true if variable can be incremented and produce next test case
    fn has_next(&self) -> bool;

    /// returns variable name to display it`s current state in logs
    fn name(&self) -> String;

    /// returns value in string format for display reasons
    fn value(&self) -> String;

    /// returns if this variable has several different values
    fn is_dynamic(&self) -> bool;
}

clone_trait_object!(DynamicVariableTrait);

/// Represents variable experiment alternatives for strings
/// Can contain single string values and list of values
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DynamicVariable<T> {
    /// current step
    pub current: usize,
    /// all test cases taken into account
    pub values: Vec<T>,
    // varable config name
    pub name: String,
}

impl<T> std::ops::Deref for DynamicVariable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.values[self.current]
    }
}

impl<T: std::fmt::Debug + std::clone::Clone + std::fmt::Display> DynamicVariableTrait for DynamicVariable<T> {
    /// Increment config variable
    fn next(&mut self) {
        if !self.has_next() {
            return;
        }

        self.current += 1;
    }

    /// Reset this variable to initial value
    fn reset(&mut self) {
        self.current = 0;
    }

    /// Returns true if variable can be incremented and produce next test case
    fn has_next(&self) -> bool {
        self.current + 1 < self.values.len()
    }

    /// returns variable name to display it`s current state in logs
    fn name(&self) -> String {
        self.name.clone()
    }

    /// returns variable name to display it`s current state in logs
    fn value(&self) -> String {
        self.values[self.current].to_string()
    }

    /// returns if this variable has several different values
    fn is_dynamic(&self) -> bool {
        self.values.len() > 1
    }
}

pub fn make_dynamic_numeric_variable<
    T: FromStr + std::fmt::Display + std::cmp::PartialOrd<T> + Default + std::ops::AddAssign + Clone,
>(
    name: String,
    config: NumericParam<T>,
) -> DynamicVariable<T> {
    match config {
        NumericParam::Value(value) => DynamicVariable {
            current: 0,
            values: vec![value],
            name,
        },
        NumericParam::Range { from, to, step } => {
            let mut values = Vec::new();
            let mut current = from;
            while current.clone() <= to {
                values.push(current.clone());
                current += step.clone();
            }

            DynamicVariable {
                current: 0,
                values,
                name,
            }
        }
        NumericParam::Values(values) => DynamicVariable {
            current: 0,
            values,
            name,
        },
    }
}

pub fn make_dynamic_custom_variable<T: FromStr + std::fmt::Display>(
    name: String,
    config: CustomParam<T>,
) -> DynamicVariable<T> {
    match config {
        CustomParam::Value(value) => DynamicVariable {
            current: 0,
            values: vec![value],
            name,
        },
        CustomParam::Values(values) => DynamicVariable {
            current: 0,
            values,
            name,
        },
    }
}
