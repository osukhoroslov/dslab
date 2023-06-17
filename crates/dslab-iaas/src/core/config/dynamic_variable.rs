//! Dynamic variables implementation.

use std::fmt::Debug;
use std::str::FromStr;

use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

pub trait DynamicVariable: Debug + DynClone {
    /// Increment config variable
    fn next(&mut self);

    /// Returns true if variable can be incremented and produce next test case
    fn has_next(&self) -> bool;

    /// Checks if variable is dynamic and can accumulate multiple values
    fn is_dynamic(&self) -> bool;
}

clone_trait_object!(DynamicVariable);

/// Represents variable experiment alternatives for integers
/// Example: 2.0,4.0,0.5 means values {2.0, 2.5, 3.0, 3.5} will be passed
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DynamicNumericVariable<T> {
    /// exact value. Has the first priority before loop
    pub value: Option<T>,
    /// current variable value (in loop mode)
    pub current: T,
    /// start variable value (from)
    pub from: Option<T>,
    /// finish variable value (to)
    pub to: Option<T>,
    /// loop incremental step
    pub step: Option<T>,
}

impl<T: FromStr + Copy + std::fmt::Display + std::cmp::PartialOrd<T> + Default + std::ops::AddAssign>
    DynamicNumericVariable<T>
{
    pub fn from_numeric(value: T) -> Self
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        Self {
            value: Some(value),
            current: value,
            from: None,
            to: None,
            step: None,
        }
    }

    pub fn from_opt_str(options_str: Option<String>) -> Option<Self>
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        options_str.as_ref()?;

        DynamicNumericVariable::<T>::from_str(&options_str.unwrap())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(options_str: &str) -> Option<Self>
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let parsed_opt = DynamicNumericVariable::<T>::parse_int_variable(options_str);
        parsed_opt.as_ref()?;

        let parsed = Box::new(parsed_opt.unwrap());
        if parsed.len() == 1 {
            return Some(Self {
                value: Some(*parsed.get(0).unwrap()),
                current: *parsed.get(0).unwrap(),
                from: None,
                to: None,
                step: None,
            });
        }

        let from = *parsed.get(0).unwrap();
        let to = *parsed.get(1).unwrap();
        let step = *parsed.get(2).unwrap();

        if (from > to && step > T::default()) || (step == T::default()) || (from < to && step < T::default()) {
            panic!(
                "Incorrect dynamic config variables: from = {}, to = {}, step = {}",
                from, to, step
            );
        }

        Some(Self {
            value: None,
            current: from,
            from: Some(from),
            to: Some(to),
            step: Some(step),
        })
    }

    /// Optional convert config string into vector of three int varaibles
    pub fn parse_int_variable(options_str: &str) -> Option<Vec<T>>
    where
        <T as FromStr>::Err: std::fmt::Debug,
    {
        let binding = options_str.replace(['[', ']'], "");
        let split = binding.split(',').collect::<Vec<&str>>();
        if split.len() == 1 {
            let binding = split.first().unwrap().replace(' ', "");
            if let Err(_e) = T::from_str(&binding) {
                return None;
            }
            return Some(vec![T::from_str(&binding).unwrap()]);
        }

        if split.len() != 3 {
            return None;
        }

        let mut result = Vec::<T>::new();
        for param in split {
            let binding = param.replace(' ', "");
            if let Err(_e) = T::from_str(&binding) {
                return None;
            }
            result.push(T::from_str(&binding).unwrap());
        }

        Some(result)
    }
}

impl<T> std::ops::Deref for DynamicNumericVariable<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.current
    }
}

impl<T: FromStr + Copy + std::fmt::Display + std::cmp::PartialOrd<T> + Default + std::ops::AddAssign + Debug>
    DynamicVariable for DynamicNumericVariable<T>
{
    /// Increment config variable
    fn next(&mut self) {
        if self.value.is_some() {
            return;
        }

        if (self.step < Some(T::default()) && self.current <= self.to.unwrap())
            || (self.step > Some(T::default()) && self.current >= self.to.unwrap())
        {
            return;
        }

        self.current += self.step.unwrap();
    }

    /// Returns true if variable can be incremented and produce next test case
    fn has_next(&self) -> bool {
        if self.value.is_some() {
            return false;
        }

        (self.step < Some(T::default()) && self.current > self.to.unwrap())
            || (self.step > Some(T::default()) && self.current < self.to.unwrap())
    }

    /// Checks if variable is dynamic and can accumulate multiple values
    fn is_dynamic(&self) -> bool {
        self.step.is_some()
    }
}
