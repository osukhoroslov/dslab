//! Dynamic simulation config which produces series of different configs.

use std::cell::RefCell;
use std::rc::Rc;

use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};

use crate::core::config::dynamic_variable::{DynamicNumericVariable, DynamicVariable};
use crate::core::config::sim_config::{HostConfig, SchedulerConfig, SimulationConfig, VmDatasetConfig};

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

/// Represents simulation configuration.
#[derive(Debug, Clone)]
pub struct ConfigState {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// message trip time from any host to any direction
    pub message_delay: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// vm initialization duration
    pub vm_start_duration: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// vm deallocation duration
    pub vm_stop_duration: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: bool,
    /// currently used to define VM migration duration
    pub network_throughput: Rc<RefCell<DynamicNumericVariable<u64>>>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// duration beetween user access the simulation info
    pub step_duration: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Rc<RefCell<DynamicNumericVariable<f64>>>,
    /// Dataset of virtual machines
    pub trace: Option<VmDatasetConfig>,
    /// cloud physical hosts
    pub hosts: Vec<HostConfig>,
    /// cloud schedulers
    pub schedulers: Vec<SchedulerConfig>,
}

/// Represents simulation configuration.
#[derive(Debug, Clone)]
pub struct ExperimentConfig {
    /// config value
    pub current_state: ConfigState,
    /// dynamic variables which will result in multiple test cases
    pub dynamic_variables: Vec<Rc<RefCell<dyn DynamicVariable>>>,
}

impl ExperimentConfig {
    /// Creates simulation config with default parameter values.
    pub fn new() -> Self {
        Self {
            current_state: ConfigState {
                send_stats_period: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(0.5))),
                message_delay: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(0.2))),
                allocation_retry_period: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(1.0))),
                vm_start_duration: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(1.))),
                vm_stop_duration: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(0.5))),
                allow_vm_overcommit: false,
                network_throughput: rc!(refcell!(DynamicNumericVariable::<u64>::from_numeric(1))),
                simulation_length: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(0.))),
                step_duration: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(500.))),
                vm_allocation_timeout: rc!(refcell!(DynamicNumericVariable::<f64>::from_numeric(50.))),
                trace: None,
                hosts: Vec::new(),
                schedulers: Vec::new(),
            },
            dynamic_variables: Vec::new(),
        }
    }

    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let current_state: ConfigDataRaw = serde_yaml::from_str(
            &std::fs::read_to_string(file_name).unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file_name));
        let default = ExperimentConfig::new().current_state;

        let send_stats_period = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.send_stats_period
        )
        .unwrap_or_else(|| default.send_stats_period.borrow().clone())));
        let message_delay = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.message_delay
        )
        .unwrap_or_else(|| default.message_delay.borrow().clone())));
        let allocation_retry_period = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.allocation_retry_period
        )
        .unwrap_or_else(|| default.allocation_retry_period.borrow().clone())));
        let vm_start_duration = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.vm_start_duration
        )
        .unwrap_or_else(|| default.vm_start_duration.borrow().clone())));
        let vm_stop_duration = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.vm_stop_duration
        )
        .unwrap_or_else(|| default.vm_stop_duration.borrow().clone())));
        let network_throughput = rc!(refcell!(DynamicNumericVariable::<u64>::from_opt_str(
            current_state.network_throughput
        )
        .unwrap_or_else(|| default.network_throughput.borrow().clone())));
        let simulation_length = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.simulation_length
        )
        .unwrap_or_else(|| default.simulation_length.borrow().clone())));
        let step_duration = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.step_duration
        )
        .unwrap_or_else(|| default.step_duration.borrow().clone())));
        let vm_allocation_timeout = rc!(refcell!(DynamicNumericVariable::<f64>::from_opt_str(
            current_state.vm_allocation_timeout
        )
        .unwrap_or_else(|| default.vm_allocation_timeout.borrow().clone())));

        let current_state = ConfigState {
            send_stats_period,
            message_delay,
            allocation_retry_period,
            vm_start_duration,
            vm_stop_duration,
            allow_vm_overcommit: current_state.allow_vm_overcommit.unwrap_or(default.allow_vm_overcommit),
            network_throughput,
            simulation_length,
            step_duration,
            vm_allocation_timeout,
            trace: current_state.trace,
            hosts: current_state.hosts.unwrap_or_default(),
            schedulers: current_state.schedulers.unwrap_or_default(),
        };

        let mut dynamic_variables = Vec::<Rc<RefCell<dyn DynamicVariable>>>::new();
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

        if dynamic_variables.len() > 1 {
            panic!("Multiple dynamic variables still not supported :(");
        }

        Self {
            current_state,
            dynamic_variables,
        }
    }

    /// Returns if some test cases are remaining
    pub fn has_next(&self) -> bool {
        if self.dynamic_variables.is_empty() {
            return false;
        }

        self.dynamic_variables.get(0).unwrap().borrow().has_next()
    }

    /// Get current config state for external usage
    pub fn get(&self) -> SimulationConfig {
        SimulationConfig {
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
            trace: self.current_state.trace.clone(),
            hosts: self.current_state.hosts.clone(),
            schedulers: self.current_state.schedulers.clone(),
        }
    }

    /// Switch to next test case
    pub fn next(&mut self) {
        if !self.has_next() {
            return;
        }

        self.dynamic_variables.get_mut(0).unwrap().borrow_mut().next();
    }
}

impl Default for ExperimentConfig {
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
