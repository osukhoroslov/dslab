//! Dynamic variables implementation.

use std::fmt::Debug;
use std::ops::AddAssign;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum NumericValues<T: PartialOrd + AddAssign + Copy> {
    Value(T),
    Range { from: T, to: T, step: T },
    Values(Vec<T>),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum GenericValues<T> {
    Value(T),
    Values(Vec<T>),
}

/// Trait for implementation of dynamic variables holding multiple values.
pub trait DynVar {
    /// Returns the variable name.
    fn name(&self) -> String;

    /// Returns true if contains multiple values.
    fn has_multiple_values(&self) -> bool;

    /// Tries to change the current value to the next value and returns true if succeeded.
    fn next(&mut self) -> bool;

    /// Resets the current value to the initial value.
    fn reset(&mut self);

    /// Returns the current value in string format for debug purposes.
    fn value(&self) -> String;
}

/// Generic dynamic variable implementation, which is used
/// to support multiple parameter values in IaaS experiments.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct GenericDynVar<T: Clone> {
    name: String,
    values: Vec<T>,
    cur_idx: usize,
}

impl<T: Clone> GenericDynVar<T> {
    pub fn new<S>(name: S, values: GenericValues<T>) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let values = match values {
            GenericValues::Value(value) => vec![value],
            GenericValues::Values(values) => values,
        };
        Self {
            name,
            values,
            cur_idx: 0,
        }
    }

    pub fn value(&self) -> T {
        self.values[self.cur_idx].clone()
    }
}

impl<T: PartialOrd + AddAssign + Copy> GenericDynVar<T> {
    pub fn from_numeric<S>(name: S, values: NumericValues<T>) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        let values = match values {
            NumericValues::Value(value) => vec![value],
            NumericValues::Range { from, to, step } => {
                let mut values = Vec::new();
                let mut current = from;
                while current <= to {
                    values.push(current);
                    current += step;
                }
                values
            }
            NumericValues::Values(values) => values,
        };
        Self {
            name,
            values,
            cur_idx: 0,
        }
    }
}

impl<T: Clone + Debug> DynVar for GenericDynVar<T> {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn has_multiple_values(&self) -> bool {
        self.values.len() > 1
    }

    fn next(&mut self) -> bool {
        if self.cur_idx + 1 < self.values.len() {
            self.cur_idx += 1;
            true
        } else {
            false
        }
    }

    fn reset(&mut self) {
        self.cur_idx = 0;
    }

    fn value(&self) -> String {
        format!("{:?}", self.values[self.cur_idx])
    }
}
