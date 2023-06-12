//! Simulation configuration.

use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

use crate::extensions::dataset_type::VmDatasetType;

pub trait DynamicVariable: Debug + DynClone {
    /// Increment config variable
    fn increment(&mut self) -> bool;

    /// Returns true if variable can be incremented and produce next test case
    fn can_increment(&self) -> bool;

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
    fn increment(&mut self) -> bool {
        if self.value.is_some() {
            return false;
        }

        if (self.step < Some(T::default()) && self.current <= self.to.unwrap())
            || (self.step > Some(T::default()) && self.current >= self.to.unwrap())
        {
            return false;
        }

        self.current += self.step.unwrap();
        true
    }

    /// Returns true if variable can be incremented and produce next test case
    fn can_increment(&self) -> bool {
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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigDataRaw {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Option<String>,
    /// message trip time from any host to any direction
    pub message_delay: Option<String>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Option<String>,
    /// vm initialization duration
    pub vm_start_duration: Option<String>,
    /// vm deallocation duration
    pub vm_stop_duration: Option<String>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: Option<bool>,
    /// currently used to define VM migration duration
    pub network_throughput: Option<String>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Option<String>,
    /// duration beetween user access the simulation info
    pub step_duration: Option<String>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Option<String>,
    /// Dataset of virtual machines
    pub trace: Option<VmDatasetConfig>,
    /// cloud physical hosts
    pub hosts: Option<Vec<HostConfig>>,
    /// cloud schedulers
    pub schedulers: Option<Vec<SchedulerConfig>>,
}

/// Represents virtual machines dataset supported by this framework.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct VmDatasetConfig {
    /// dataset type, one of supported by dslab framework
    pub r#type: VmDatasetType,
    /// dataset file path where data is stored
    pub path: String,
}

/// Represents scheduler(s) configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchedulerConfig {
    /// Scheduler name. Should be set if count = 1
    pub name: Option<String>,
    /// Scheduler name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// VM placement algorithm for this scheduler
    pub algorithm: String,
    /// number of such schedulers
    pub count: Option<u32>,
}

/// Represents physical host(s) configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct HostConfig {
    /// Host name. Should be set if count = 1
    pub name: Option<String>,
    /// Host name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// host CPU capacity
    pub cpus: u32,
    /// host memory capacity
    pub memory: u64,
    /// number of such hosts
    pub count: Option<u32>,
}

/// Represents simulation configuration.
#[derive(Debug, Clone)]
pub struct ConfigData {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: DynamicNumericVariable<f64>,
    /// message trip time from any host to any direction
    pub message_delay: DynamicNumericVariable<f64>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: DynamicNumericVariable<f64>,
    /// vm initialization duration
    pub vm_start_duration: DynamicNumericVariable<f64>,
    /// vm deallocation duration
    pub vm_stop_duration: DynamicNumericVariable<f64>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: bool,
    /// currently used to define VM migration duration
    pub network_throughput: DynamicNumericVariable<u64>,
    /// length of simulation (for public datasets only)
    pub simulation_length: DynamicNumericVariable<f64>,
    /// duration beetween user access the simulation info
    pub step_duration: DynamicNumericVariable<f64>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: DynamicNumericVariable<f64>,
    /// Dataset of virtual machines
    pub trace: Option<VmDatasetConfig>,
    /// cloud physical hosts
    pub hosts: Vec<HostConfig>,
    /// cloud schedulers
    pub schedulers: Vec<SchedulerConfig>,
}

/// Represents simulation configuration.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// config value
    pub data: ConfigData,
    /// dynamic variables which will result in multiple test cases
    pub dynamic_variables: Vec<Box<dyn DynamicVariable>>,
}

impl SimulationConfig {
    /// Creates simulation config with default parameter values.
    pub fn new() -> Self {
        Self {
            data: ConfigData {
                send_stats_period: DynamicNumericVariable::<f64>::from_numeric(0.5),
                message_delay: DynamicNumericVariable::<f64>::from_numeric(0.2),
                allocation_retry_period: DynamicNumericVariable::<f64>::from_numeric(1.0),
                vm_start_duration: DynamicNumericVariable::<f64>::from_numeric(1.),
                vm_stop_duration: DynamicNumericVariable::<f64>::from_numeric(0.5),
                allow_vm_overcommit: false,
                network_throughput: DynamicNumericVariable::<u64>::from_numeric(1),
                simulation_length: DynamicNumericVariable::<f64>::from_numeric(0.),
                step_duration: DynamicNumericVariable::<f64>::from_numeric(500.),
                vm_allocation_timeout: DynamicNumericVariable::<f64>::from_numeric(50.),
                trace: None,
                hosts: Vec::new(),
                schedulers: Vec::new(),
            },
            dynamic_variables: Vec::new(),
        }
    }

    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let data: ConfigDataRaw = serde_yaml::from_str(
            &std::fs::read_to_string(file_name).unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file_name));
        let default = SimulationConfig::new().data;

        let mut dynamic_variables = Vec::<Box<dyn DynamicVariable>>::new();

        let send_stats_period =
            DynamicNumericVariable::<f64>::from_opt_str(data.send_stats_period).unwrap_or(default.send_stats_period);
        if send_stats_period.is_dynamic() {
            dynamic_variables.push(Box::new(send_stats_period.clone()));
        }
        let message_delay =
            DynamicNumericVariable::<f64>::from_opt_str(data.message_delay).unwrap_or(default.message_delay);
        if message_delay.is_dynamic() {
            dynamic_variables.push(Box::new(message_delay.clone()));
        }
        let allocation_retry_period = DynamicNumericVariable::<f64>::from_opt_str(data.allocation_retry_period)
            .unwrap_or(default.allocation_retry_period);
        if allocation_retry_period.is_dynamic() {
            dynamic_variables.push(Box::new(allocation_retry_period.clone()));
        }
        let vm_start_duration =
            DynamicNumericVariable::<f64>::from_opt_str(data.vm_start_duration).unwrap_or(default.vm_start_duration);
        if vm_start_duration.is_dynamic() {
            dynamic_variables.push(Box::new(vm_start_duration.clone()));
        }
        let vm_stop_duration =
            DynamicNumericVariable::<f64>::from_opt_str(data.vm_stop_duration).unwrap_or(default.vm_stop_duration);
        if vm_stop_duration.is_dynamic() {
            dynamic_variables.push(Box::new(vm_stop_duration.clone()));
        }
        let network_throughput =
            DynamicNumericVariable::<u64>::from_opt_str(data.network_throughput).unwrap_or(default.network_throughput);
        if network_throughput.is_dynamic() {
            dynamic_variables.push(Box::new(network_throughput.clone()));
        }
        let simulation_length =
            DynamicNumericVariable::<f64>::from_opt_str(data.simulation_length).unwrap_or(default.simulation_length);
        if simulation_length.is_dynamic() {
            dynamic_variables.push(Box::new(simulation_length.clone()));
        }
        let step_duration =
            DynamicNumericVariable::<f64>::from_opt_str(data.step_duration).unwrap_or(default.step_duration);
        if step_duration.is_dynamic() {
            dynamic_variables.push(Box::new(step_duration.clone()));
        }
        let vm_allocation_timeout = DynamicNumericVariable::<f64>::from_opt_str(data.vm_allocation_timeout)
            .unwrap_or(default.vm_allocation_timeout);
        if vm_allocation_timeout.is_dynamic() {
            dynamic_variables.push(Box::new(vm_allocation_timeout.clone()));
        }

        if dynamic_variables.len() > 1 {
            panic!("Multiple dynamic variables still not supported :(");
        }

        Self {
            data: ConfigData {
                send_stats_period,
                message_delay,
                allocation_retry_period,
                vm_start_duration,
                vm_stop_duration,
                allow_vm_overcommit: data.allow_vm_overcommit.unwrap_or(default.allow_vm_overcommit),
                network_throughput,
                simulation_length,
                step_duration,
                vm_allocation_timeout,
                trace: data.trace,
                hosts: data.hosts.unwrap_or_default(),
                schedulers: data.schedulers.unwrap_or_default(),
            },
            dynamic_variables,
        }
    }

    /// Returns total hosts count
    pub fn number_of_hosts(&self) -> u32 {
        let mut result = 0;
        for host in self.data.hosts.clone().into_iter() {
            result += host.count.unwrap_or(1);
        }
        result
    }

    /// Returns if some test cases are remaining
    pub fn can_increment(&self) -> bool {
        if self.dynamic_variables.is_empty() {
            return false;
        }

        self.dynamic_variables.get(0).unwrap().can_increment()
    }

    /// Switch to next test case
    pub fn increment(&mut self) {
        if !self.can_increment() {
            return;
        }

        self.dynamic_variables.get_mut(0).unwrap().increment();
    }
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses config value string, which consists of two parts - name and options.
/// Example: ConstLoadModel[load=0.8] parts are name ConstLoadModel and options string "load=0.8".
pub fn parse_config_value(config_str: &str) -> (String, Option<String>) {
    match config_str.split_once('[') {
        Some((l, r)) => (l.to_string(), Some(r.to_string().replace(']', ""))),
        None => (config_str.to_string(), None),
    }
}

/// Parses options string from config value, returns map with option names and values.
///
/// # Examples
///
/// ```rust
/// use dslab_iaas::core::config::parse_options;
///
/// let options = parse_options("option1=0.8,option2=something");
/// assert_eq!(options.get("option1").unwrap(), "0.8");
/// assert_eq!(options.get("option2").unwrap(), "something");
/// assert_eq!(options.get("option3"), None);
/// ```
pub fn parse_options(options_str: &str) -> HashMap<String, String> {
    let mut options = HashMap::new();
    for option_str in options_str.split(',') {
        if let Some((name, value)) = option_str.split_once('=') {
            options.insert(name.to_string(), value.to_string());
        }
    }
    options
}
