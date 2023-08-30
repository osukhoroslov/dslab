//! Dynamic simulation config which produces series of different configs.

use std::cell::RefCell;
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};

use crate::core::config::dynamic_variable::{DynVar, GenericDynVar, GenericValues, NumericValues};
use crate::core::config::sim_config::{HostConfig, SchedulerConfig, SimulationConfig, VmDatasetConfig};

/// Represents scheduler(s) configuration.
#[derive(Serialize, Deserialize)]
pub struct SchedulerConfigRaw {
    /// Scheduler name. Should be set if count = 1
    pub name: Option<String>,
    /// Scheduler name prefix. Full name is produced by appending instance number to the prefix.
    /// Should be set if count > 1
    pub name_prefix: Option<String>,
    /// VM placement algorithm for this scheduler
    pub algorithm: GenericValues<String>,
    /// number of such schedulers
    pub count: Option<NumericValues<u32>>,
}

#[derive(Serialize, Deserialize)]
pub struct ConfigDataRaw {
    /// periodically send statistics from host to monitoring
    pub send_stats_period: Option<NumericValues<f64>>,
    /// message trip time from any host to any direction
    pub message_delay: Option<NumericValues<f64>>,
    /// when allocation request fails then wait for this duration
    pub allocation_retry_period: Option<NumericValues<f64>>,
    /// vm initialization duration
    pub vm_start_duration: Option<NumericValues<f64>>,
    /// vm deallocation duration
    pub vm_stop_duration: Option<NumericValues<f64>>,
    /// pack VM by real resource consumption, not SLA
    pub allow_vm_overcommit: Option<bool>,
    /// currently used to define VM migration duration
    pub network_throughput: Option<NumericValues<u64>>,
    /// length of simulation (for public datasets only)
    pub simulation_length: Option<NumericValues<f64>>,
    /// duration between user access the simulation info
    pub step_duration: Option<NumericValues<f64>>,
    /// VM becomes failed after this timeout is reached
    pub vm_allocation_timeout: Option<NumericValues<f64>>,
    /// Dataset of virtual machines
    pub trace: Option<GenericValues<VmDatasetConfig>>,
    /// cloud physical hosts
    pub hosts: Option<Vec<HostConfig>>,
    /// cloud schedulers
    pub schedulers: Option<Vec<SchedulerConfigRaw>>,
}

#[derive(Debug)]
struct SchedulerConfigState {
    pub name: Option<String>,
    pub name_prefix: Option<String>,
    pub algorithm: Rc<RefCell<GenericDynVar<String>>>,
    pub count: Rc<RefCell<GenericDynVar<u32>>>,
}

#[derive(Debug)]
struct ConfigState {
    pub send_stats_period: Rc<RefCell<GenericDynVar<f64>>>,
    pub message_delay: Rc<RefCell<GenericDynVar<f64>>>,
    pub allocation_retry_period: Rc<RefCell<GenericDynVar<f64>>>,
    pub vm_start_duration: Rc<RefCell<GenericDynVar<f64>>>,
    pub vm_stop_duration: Rc<RefCell<GenericDynVar<f64>>>,
    pub allow_vm_overcommit: bool,
    pub network_throughput: Rc<RefCell<GenericDynVar<u64>>>,
    pub simulation_length: Rc<RefCell<GenericDynVar<f64>>>,
    pub step_duration: Rc<RefCell<GenericDynVar<f64>>>,
    pub vm_allocation_timeout: Rc<RefCell<GenericDynVar<f64>>>,
    pub trace: Option<Rc<RefCell<GenericDynVar<VmDatasetConfig>>>>,
    pub hosts: Vec<HostConfig>,
    pub schedulers: Vec<SchedulerConfigState>,
}

/// Represents simulation configuration.
pub struct ExperimentConfig {
    current_state: ConfigState,
    dyn_vars: Vec<Rc<RefCell<dyn DynVar>>>,
    initial_state: bool,
}

impl ExperimentConfig {
    /// Creates simulation config by reading parameter values from .yaml file (uses default values if some parameters are absent).
    pub fn from_file(file_name: &str) -> Self {
        let current_state_raw: ConfigDataRaw = serde_yaml::from_str(
            &std::fs::read_to_string(file_name).unwrap_or_else(|_| panic!("Can't read file {}", file_name)),
        )
        .unwrap_or_else(|err| panic!("Can't parse YAML from file {}: {}", file_name, err));

        let mut dyn_vars = Vec::<Rc<RefCell<dyn DynVar>>>::new();

        let send_stats_period = rc!(refcell!(GenericDynVar::from_numeric(
            "send_stats_period",
            current_state_raw.send_stats_period.unwrap_or(NumericValues::Value(0.5))
        )));
        if send_stats_period.borrow().has_multiple_values() {
            dyn_vars.push(send_stats_period.clone());
        }

        let message_delay = rc!(refcell!(GenericDynVar::from_numeric(
            "message_delay",
            current_state_raw.message_delay.unwrap_or(NumericValues::Value(0.2))
        )));
        if message_delay.borrow().has_multiple_values() {
            dyn_vars.push(message_delay.clone());
        }

        let allocation_retry_period = rc!(refcell!(GenericDynVar::from_numeric(
            "allocation_retry_period",
            current_state_raw
                .allocation_retry_period
                .unwrap_or(NumericValues::Value(1.))
        )));
        if allocation_retry_period.borrow().has_multiple_values() {
            dyn_vars.push(allocation_retry_period.clone());
        }

        let vm_start_duration = rc!(refcell!(GenericDynVar::from_numeric(
            "vm_start_duration",
            current_state_raw.vm_start_duration.unwrap_or(NumericValues::Value(1.))
        )));
        if vm_start_duration.borrow().has_multiple_values() {
            dyn_vars.push(vm_start_duration.clone());
        }

        let vm_stop_duration = rc!(refcell!(GenericDynVar::from_numeric(
            "vm_stop_duration",
            current_state_raw.vm_stop_duration.unwrap_or(NumericValues::Value(0.5))
        )));
        if vm_stop_duration.borrow().has_multiple_values() {
            dyn_vars.push(vm_stop_duration.clone());
        }

        let network_throughput = rc!(refcell!(GenericDynVar::from_numeric(
            "network_throughput",
            current_state_raw.network_throughput.unwrap_or(NumericValues::Value(1))
        )));
        if network_throughput.borrow().has_multiple_values() {
            dyn_vars.push(network_throughput.clone());
        }

        let simulation_length = rc!(refcell!(GenericDynVar::from_numeric(
            "simulation_length",
            current_state_raw.simulation_length.unwrap_or(NumericValues::Value(0.))
        )));
        if simulation_length.borrow().has_multiple_values() {
            dyn_vars.push(simulation_length.clone());
        }

        let step_duration = rc!(refcell!(GenericDynVar::from_numeric(
            "step_duration",
            current_state_raw.step_duration.unwrap_or(NumericValues::Value(500.))
        )));
        if step_duration.borrow().has_multiple_values() {
            dyn_vars.push(step_duration.clone());
        }

        let vm_allocation_timeout = rc!(refcell!(GenericDynVar::from_numeric(
            "vm_allocation_timeout",
            current_state_raw
                .vm_allocation_timeout
                .unwrap_or(NumericValues::Value(50.))
        )));
        if vm_allocation_timeout.borrow().has_multiple_values() {
            dyn_vars.push(vm_allocation_timeout.clone());
        }

        let trace: Option<Rc<RefCell<GenericDynVar<VmDatasetConfig>>>> = current_state_raw
            .trace
            .map(|raw_trace| rc!(refcell!(GenericDynVar::new("trace", raw_trace))));
        if trace.is_some() && trace.as_ref().unwrap().borrow().has_multiple_values() {
            dyn_vars.push(trace.clone().unwrap());
        }

        let mut schedulers: Vec<SchedulerConfigState> = Vec::new();
        for scheduler in current_state_raw.schedulers.unwrap_or_default() {
            let algorithm = rc!(refcell!(GenericDynVar::new("algorithm", scheduler.algorithm)));
            let count = rc!(refcell!(GenericDynVar::from_numeric(
                "count",
                scheduler.count.unwrap_or(NumericValues::Value(1)),
            )));

            if algorithm.borrow().has_multiple_values() {
                dyn_vars.push(algorithm.clone());
            }
            if count.borrow().has_multiple_values() {
                dyn_vars.push(count.clone());
            }

            schedulers.push(SchedulerConfigState {
                name: scheduler.name,
                name_prefix: scheduler.name_prefix,
                algorithm,
                count,
            });
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

        Self {
            current_state,
            dyn_vars,
            initial_state: true,
        }
    }

    /// Get current config state for external usage
    pub fn get(&mut self) -> Option<SimulationConfig> {
        if !self.next() {
            return None;
        }

        let mut schedulers: Vec<SchedulerConfig> = Vec::new();
        for scheduler in &*self.current_state.schedulers {
            schedulers.push(SchedulerConfig {
                name: scheduler.name.clone(),
                name_prefix: scheduler.name_prefix.clone(),
                algorithm: scheduler.algorithm.borrow().value(),
                count: scheduler.count.borrow().value(),
            });
        }

        let mut trace: Option<VmDatasetConfig> = None;
        if self.current_state.trace.is_some() {
            trace = Some(self.current_state.trace.as_ref().unwrap().borrow().value());
        }

        Some(SimulationConfig {
            send_stats_period: self.current_state.send_stats_period.borrow().value(),
            message_delay: self.current_state.message_delay.borrow().value(),
            allocation_retry_period: self.current_state.allocation_retry_period.borrow().value(),
            vm_start_duration: self.current_state.vm_start_duration.borrow().value(),
            vm_stop_duration: self.current_state.vm_stop_duration.borrow().value(),
            allow_vm_overcommit: self.current_state.allow_vm_overcommit,
            network_throughput: self.current_state.network_throughput.borrow().value(),
            simulation_length: self.current_state.simulation_length.borrow().value(),
            step_duration: self.current_state.step_duration.borrow().value(),
            vm_allocation_timeout: self.current_state.vm_allocation_timeout.borrow().value(),
            trace,
            hosts: self.current_state.hosts.clone(),
            schedulers,
        })
    }

    /// Switch to next test case
    fn next(&mut self) -> bool {
        if self.initial_state {
            self.initial_state = false;
            return true;
        }

        for i in 0..self.dyn_vars.len() {
            let mut var = self.dyn_vars[i].borrow_mut();
            if var.next() {
                return true;
            }
            var.reset();
        }

        // no cases left
        false
    }
}

/// Print experiment current state with dynamic variables values
impl Debug for ExperimentConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = f.debug_struct("Experiment state");
        for var in &self.dyn_vars {
            let name = var.borrow().name();
            let value = var.borrow().value();
            result.field(&name, &value);
        }
        result.finish()
    }
}
