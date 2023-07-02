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

pub trait DynamicVariable: Debug + DynClone {
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

clone_trait_object!(DynamicVariable);

/// Represents variable experiment alternatives for integers
/// Example: 2.0,4.0,0.5 means values {2.0, 2.5, 3.0, 4.0} will be passed
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DynamicNumericVariable<T> {
    /// current step
    pub current: usize,
    /// all test cases taken into account
    pub values: Vec<T>,
    // varable config name
    pub name: String,
}

impl<T: FromStr + Copy + std::fmt::Display + std::cmp::PartialOrd<T> + Default + std::ops::AddAssign>
    DynamicNumericVariable<T>
{
    pub fn from_param(name: String, config: NumericParam<T>) -> Self
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        match config {
            NumericParam::Value(value) => Self {
                current: 0,
                values: vec![value],
                name,
            },
            NumericParam::Range { from, to, step } => {
                let mut values = Vec::new();
                let mut current = from;
                while current <= to {
                    values.push(current);
                    current += step;
                }

                Self {
                    current: 0,
                    values,
                    name,
                }
            }
            NumericParam::Values(values) => Self {
                current: 0,
                values,
                name,
            },
        }
    }
}

impl<T> std::ops::Deref for DynamicNumericVariable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.values[self.current]
    }
}

impl<T: FromStr + Copy + std::fmt::Display + std::cmp::PartialOrd<T> + Default + std::ops::AddAssign + Debug>
    DynamicVariable for DynamicNumericVariable<T>
{
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
        format!("{}", self.values[self.current])
    }

    /// returns if this variable has several different values
    fn is_dynamic(&self) -> bool {
        self.values.len() > 1
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum StringParam {
    Value(String),
    Values(Vec<String>),
}

/// Represents variable experiment alternatives for strings
/// Can contain single string values and list of values
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DynamicStringVariable {
    /// current step
    pub current: usize,
    /// all test cases taken into account
    pub values: Vec<String>,
    // varable config name
    pub name: String,
}

impl DynamicStringVariable {
    pub fn from_param(name: String, config: StringParam) -> Self {
        match config {
            StringParam::Value(value) => Self {
                current: 0,
                values: vec![value],
                name,
            },
            StringParam::Values(values) => Self {
                current: 0,
                values,
                name,
            },
        }
    }
}

impl std::ops::Deref for DynamicStringVariable {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.values[self.current]
    }
}

impl DynamicVariable for DynamicStringVariable {
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
