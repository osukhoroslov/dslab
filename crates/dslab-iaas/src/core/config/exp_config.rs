//! Dynamic simulation config which produces series of different configs.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};

use crate::core::config::dynamic_variable::{
    make_dynamic_custom_variable, make_dynamic_numeric_variable, CustomParam, DynamicVariable, DynamicVariableTrait,
    NumericParam,
};
use crate::core::config::sim_config::{HostConfig, SchedulerConfig, SimulationConfig, VmDatasetConfig};

/// Represents scheduler(s) configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchedulerConfigRaw {
    /// Scheduler name. Should be set if count = 1
    pub name: Option<String>,
    /// Scheduler name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// VM placement algorithm for this scheduler
    pub algorithm: CustomParam<String>,
    /// number of such schedulers
    pub count: Option<NumericParam<u32>>,
}

/// Represents scheduler(s) configuration.
#[derive(Debug, PartialEq, Clone)]
pub struct SchedulerConfigState {
    /// Scheduler name. Should be set if count = 1
    pub name: Option<String>,
    /// Scheduler name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// VM placement algorithm for this scheduler
    pub algorithm: Rc<RefCell<DynamicVariable<String>>>,
    /// number of such schedulers
    pub count: Rc<RefCell<DynamicVariable<u32>>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ConfigDataRaw {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Option<NumericParam<f64>>,
    /// message trip time from any host to any direction
    pub message_delay: Option<NumericParam<f64>>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Option<NumericParam<f64>>,
    /// vm initialization duration
    pub vm_start_duration: Option<NumericParam<f64>>,
    /// vm deallocation duration
    pub vm_stop_duration: Option<NumericParam<f64>>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: Option<bool>,
    /// currently used to define VM migration duration
    pub network_throughput: Option<NumericParam<u64>>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Option<NumericParam<f64>>,
    /// duration beetween user access the simulation info
    pub step_duration: Option<NumericParam<f64>>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Option<NumericParam<f64>>,
    /// Dataset of virtual machines
    pub trace: Option<CustomParam<VmDatasetConfig>>,
    /// cloud physical hosts
    pub hosts: Option<Vec<HostConfig>>,
    /// cloud schedulers
    pub schedulers: Option<Vec<SchedulerConfigRaw>>,
}

/// Represents simulation configuration.
#[derive(Debug, Clone)]
pub struct ConfigState {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Rc<RefCell<DynamicVariable<f64>>>,
    /// message trip time from any host to any direction
    pub message_delay: Rc<RefCell<DynamicVariable<f64>>>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Rc<RefCell<DynamicVariable<f64>>>,
    /// vm initialization duration
    pub vm_start_duration: Rc<RefCell<DynamicVariable<f64>>>,
    /// vm deallocation duration
    pub vm_stop_duration: Rc<RefCell<DynamicVariable<f64>>>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: bool,
    /// currently used to define VM migration duration
    pub network_throughput: Rc<RefCell<DynamicVariable<u64>>>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Rc<RefCell<DynamicVariable<f64>>>,
    /// duration beetween user access the simulation info
    pub step_duration: Rc<RefCell<DynamicVariable<f64>>>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Rc<RefCell<DynamicVariable<f64>>>,
    /// Dataset of virtual machines
    pub trace: Option<Rc<RefCell<DynamicVariable<VmDatasetConfig>>>>,
    /// cloud physical hosts
    pub hosts: Vec<HostConfig>,
    /// cloud schedulers
    pub schedulers: Vec<SchedulerConfigState>,
}

/// Represents simulation configuration.
#[derive(Clone)]
pub struct ExperimentConfig {
    /// config value
    pub current_state: ConfigState,
    /// dynamic variables which will result in multiple test cases
    pub dynamic_variables: Vec<Rc<RefCell<dyn DynamicVariableTrait>>>,
    /// if the next state is first
    pub initial_state: bool,
    /// if there is one more state to process
    pub has_next: bool,
}

impl ExperimentConfig {
    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let current_state_raw: ConfigDataRaw = serde_yaml::from_str(
            &std::fs::read_to_string(file_name).unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|err| panic!("Can't parse YAML from file {}: {}", file_name, err));

        let send_stats_period = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "send_stats_period".to_string(),
            current_state_raw.send_stats_period.unwrap_or(NumericParam::Value(0.5))
        )));
        let message_delay = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "message_delay".to_string(),
            current_state_raw.message_delay.unwrap_or(NumericParam::Value(0.2))
        )));
        let allocation_retry_period = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "allocation_retry_period".to_string(),
            current_state_raw
                .allocation_retry_period
                .unwrap_or(NumericParam::Value(1.))
        )));
        let vm_start_duration = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "vm_start_duration".to_string(),
            current_state_raw.vm_start_duration.unwrap_or(NumericParam::Value(1.))
        )));
        let vm_stop_duration = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "vm_stop_duration".to_string(),
            current_state_raw.vm_stop_duration.unwrap_or(NumericParam::Value(0.5))
        )));
        let network_throughput = rc!(refcell!(make_dynamic_numeric_variable::<u64>(
            "network_throughput".to_string(),
            current_state_raw.network_throughput.unwrap_or(NumericParam::Value(1))
        )));
        let simulation_length = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "simulation_length".to_string(),
            current_state_raw.simulation_length.unwrap_or(NumericParam::Value(0.))
        )));
        let step_duration = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "step_duration".to_string(),
            current_state_raw.step_duration.unwrap_or(NumericParam::Value(500.))
        )));
        let vm_allocation_timeout = rc!(refcell!(make_dynamic_numeric_variable::<f64>(
            "vm_allocation_timeout".to_string(),
            current_state_raw
                .vm_allocation_timeout
                .unwrap_or(NumericParam::Value(50.))
        )));

        let mut trace: Option<Rc<RefCell<DynamicVariable<VmDatasetConfig>>>> = None;
        if current_state_raw.trace.is_some() {
            trace = Some(rc!(refcell!(make_dynamic_custom_variable::<VmDatasetConfig>(
                "trace".to_string(),
                current_state_raw.trace.unwrap(),
            ))));
        }

        let mut algorithms: Vec<Rc<RefCell<DynamicVariable<String>>>> = Vec::new();
        let mut scheduler_counts: Vec<Rc<RefCell<DynamicVariable<u32>>>> = Vec::new();
        let mut schedulers: Vec<SchedulerConfigState> = Vec::new();
        for scheduler in current_state_raw.schedulers.unwrap_or_default() {
            let algorithm = rc!(refcell!(make_dynamic_custom_variable::<String>(
                "algorithm".to_string(),
                scheduler.algorithm
            )));
            let count = rc!(refcell!(make_dynamic_numeric_variable::<u32>(
                "count".to_string(),
                scheduler.count.unwrap_or(NumericParam::Value(1)),
            )));

            schedulers.push(SchedulerConfigState {
                name: scheduler.name,
                name_prefix: scheduler.name_prefix,
                algorithm: algorithm.clone(),
                count: count.clone(),
            });
            algorithms.push(algorithm);
            scheduler_counts.push(count);
        }

        let current_state = ConfigState {
            send_stats_period,
            message_delay,
            allocation_retry_period,
            vm_start_duration,
            vm_stop_duration,
            allow_vm_overcommit: current_state_raw.allow_vm_overcommit.unwrap_or(false),
            network_throughput,
            simulation_length,
            step_duration,
            vm_allocation_timeout,
            trace,
            hosts: current_state_raw.hosts.unwrap_or_default(),
            schedulers,
        };

        let mut dynamic_variables = Vec::<Rc<RefCell<dyn DynamicVariableTrait>>>::new();
        if current_state.send_stats_period.borrow().is_dynamic() {
            dynamic_variables.push(current_state.send_stats_period.clone());
        }
        if current_state.message_delay.borrow().is_dynamic() {
            dynamic_variables.push(current_state.message_delay.clone());
        }
        if current_state.allocation_retry_period.borrow().is_dynamic() {
            dynamic_variables.push(current_state.allocation_retry_period.clone());
        }
        if current_state.vm_start_duration.borrow().is_dynamic() {
            dynamic_variables.push(current_state.vm_start_duration.clone());
        }
        if current_state.vm_stop_duration.borrow().is_dynamic() {
            dynamic_variables.push(current_state.vm_stop_duration.clone());
        }
        if current_state.network_throughput.borrow().is_dynamic() {
            dynamic_variables.push(current_state.network_throughput.clone());
        }
        if current_state.simulation_length.borrow().is_dynamic() {
            dynamic_variables.push(current_state.simulation_length.clone());
        }
        if current_state.step_duration.borrow().is_dynamic() {
            dynamic_variables.push(current_state.step_duration.clone());
        }
        if current_state.vm_allocation_timeout.borrow().is_dynamic() {
            dynamic_variables.push(current_state.vm_allocation_timeout.clone());
        }
        if current_state.trace.is_some() && current_state.trace.clone().unwrap().borrow().is_dynamic() {
            dynamic_variables.push(current_state.trace.clone().unwrap());
        }
        for algorithm in algorithms {
            if algorithm.borrow().is_dynamic() {
                dynamic_variables.push(algorithm.clone());
            }
        }
        for count in scheduler_counts {
            if count.borrow().is_dynamic() {
                dynamic_variables.push(count.clone());
            }
        }

        Self {
            current_state,
            dynamic_variables,
            initial_state: true,
            has_next: true,
        }
    }

    /// Returns if some test cases are remaining
    fn has_next(&self) -> bool {
        for i in 0..self.dynamic_variables.len() {
            if self.dynamic_variables[i].borrow().has_next() {
                return true;
            }
        }
        false
    }

    /// Switch to next test case
    fn next(&mut self) {
        if self.initial_state {
            self.initial_state = false;
            return;
        }

        for i in 0..self.dynamic_variables.len() {
            if self.dynamic_variables[i].borrow().has_next() {
                self.dynamic_variables[i].borrow_mut().next();
                return;
            }
            self.dynamic_variables[i].borrow_mut().reset();
        }
    }

    /// Get current config state for external usage
    pub fn get(&mut self) -> Option<SimulationConfig> {
        if !self.has_next() {
            return None;
        }
        self.next();

        let mut schedulers: Vec<SchedulerConfig> = Vec::new();
        for scheduler in &*self.current_state.schedulers {
            schedulers.push(SchedulerConfig {
                name: scheduler.name.clone(),
                name_prefix: scheduler.name_prefix.clone(),
                algorithm: (**scheduler.algorithm.borrow()).to_string(),
                count: **scheduler.count.borrow(),
            });
        }

        let mut trace: Option<VmDatasetConfig> = None;
        if self.current_state.trace.is_some() {
            trace = Some((**self.current_state.trace.clone().unwrap().borrow()).clone());
        }

        Some(SimulationConfig {
            send_stats_period: **self.current_state.send_stats_period.borrow(),
            message_delay: **self.current_state.message_delay.borrow(),
            allocation_retry_period: **self.current_state.allocation_retry_period.borrow(),
            vm_start_duration: **self.current_state.vm_start_duration.borrow(),
            vm_stop_duration: **self.current_state.vm_stop_duration.borrow(),
            allow_vm_overcommit: self.current_state.allow_vm_overcommit,
            network_throughput: **self.current_state.network_throughput.borrow(),
            simulation_length: **self.current_state.simulation_length.borrow(),
            step_duration: **self.current_state.step_duration.borrow(),
            vm_allocation_timeout: **self.current_state.vm_allocation_timeout.borrow(),
            trace,
            hosts: self.current_state.hosts.clone(),
            schedulers,
        })
    }
}

/// Print experiment current state with dynamic variables values
impl fmt::Debug for ExperimentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = f.debug_struct("Experiment state");
        for variable in &self.dynamic_variables {
            let name = variable.borrow().name();
            let value = variable.borrow().value();
            result.field(&name, &value);
        }

        result.finish()
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
/// use dslab_iaas::core::config::exp_config::parse_options;
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