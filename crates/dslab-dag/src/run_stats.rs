use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::system::System;

/// Contains metrics collected from a simulation run.
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct RunStats {
    /// Makespan expected by the scheduling algorithm (for static algorithms only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_makespan: Option<f64>,
    /// Scheduling algorithm's execution time (total for all calls to the scheduler).
    pub scheduling_time: f64,
    /// Total task execution time (in seconds).
    pub total_task_time: f64,
    /// Total amount of data transmitted over the network (in MB).
    pub total_network_traffic: f64,
    /// Total time of data transfers over the network (in seconds).
    pub total_network_time: f64,
    /// Maximum number of cores used at once.
    pub max_used_cores: u32,
    /// Maximum amount of memory used at once.
    pub max_used_memory: u64,
    /// Maximum CPU utilization (max_used_cores / total_cores).
    pub max_cpu_utilization: f64,
    /// Maximum memory utilization (max_used_memory / total_memory).
    pub max_memory_utilization: f64,
    /// Average CPU utilization (the ratio of core-seconds consumed by tasks to total core-seconds).
    pub cpu_utilization: f64,
    /// Average memory utilization (analogous to cpu_utilization).
    pub memory_utilization: f64,
    /// The number of used resources, i.e. on which at least one task has been executed.
    pub used_resource_count: usize,
    /// Average CPU utilization for used resources only,
    /// i.e. unused resources are not taken into account in the denominator.
    pub cpu_utilization_used: f64,
    /// Average memory utilization for used resources only (analogous to cpu_utilization_used).
    pub memory_utilization_used: f64,
    /// Average CPU utilization for active resources only,
    /// i.e. without taking into account the consumption before the first execution of the task on the resource
    /// and after the last execution. That is, we consider resources as machines that are turned on when they are
    /// needed and turned off when they are no longer needed.
    pub cpu_utilization_active: f64,
    /// Average memory utilization for active resources only (analogous to cpu_utilization_active).
    pub memory_utilization_active: f64,

    #[serde(skip)]
    task_starts: HashMap<usize, (u32, u64, f64)>,
    #[serde(skip)]
    transfer_starts: HashMap<usize, f64>,
    #[serde(skip)]
    current_cores: u32,
    #[serde(skip)]
    current_memory: u64,
    #[serde(skip)]
    used_resources: HashSet<usize>,
    #[serde(skip)]
    task_resource: HashMap<usize, usize>,
    #[serde(skip)]
    resource_first_used: HashMap<usize, f64>,
    #[serde(skip)]
    resource_last_used: HashMap<usize, f64>,
}

impl RunStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_expected_makespan(&mut self, makespan: f64) {
        self.expected_makespan = Some(makespan);
    }

    pub fn add_scheduling_time(&mut self, time: f64) {
        self.scheduling_time += time;
    }

    pub fn set_task_start(&mut self, task: usize, resource: usize, cores: u32, memory: u64, time: f64) {
        self.current_cores += cores;
        self.max_used_cores = self.max_used_cores.max(self.current_cores);
        self.current_memory += memory;
        self.max_used_memory = self.max_used_memory.max(self.current_memory);
        self.task_starts.insert(task, (cores, memory, time));
        self.used_resources.insert(resource);
        self.task_resource.insert(task, resource);
        self.resource_first_used.entry(resource).or_insert(time);
    }

    pub fn set_task_finish(&mut self, task: usize, time: f64) {
        let (cores, memory, start_time) = self.task_starts.remove(&task).unwrap();
        self.current_cores -= cores;
        self.current_memory -= memory;
        self.total_task_time += time - start_time;
        self.cpu_utilization += (time - start_time) * cores as f64;
        self.memory_utilization += (time - start_time) * memory as f64;
        self.resource_last_used.insert(self.task_resource[&task], time);
    }

    pub fn set_transfer_start(&mut self, data_item: usize, size: f64, time: f64) {
        self.total_network_traffic += size;
        self.transfer_starts.insert(data_item, time);
    }

    pub fn set_transfer_finish(&mut self, data_item: usize, time: f64) {
        self.total_network_time += time - self.transfer_starts.remove(&data_item).unwrap();
    }

    pub fn finalize(&mut self, time: f64, system: System) {
        assert!(self.task_starts.is_empty());
        assert!(self.transfer_starts.is_empty());

        let mut total_cores = 0;
        let mut total_memory = 0;
        let mut total_cores_used = 0;
        let mut total_memory_used = 0;
        let mut total_cores_active = 0.;
        let mut total_memory_active = 0.;
        for (i, r) in system.resources.iter().enumerate() {
            total_cores += r.cores_available;
            total_memory += r.memory_available;
            if self.used_resources.contains(&i) {
                total_cores_used += r.cores_available;
                total_memory_used += r.memory_available;
                total_cores_active +=
                    r.cores_available as f64 * (self.resource_last_used[&i] - self.resource_first_used[&i]);
                total_memory_active +=
                    r.memory_available as f64 * (self.resource_last_used[&i] - self.resource_first_used[&i]);
            }
        }

        self.max_cpu_utilization = self.max_used_cores as f64 / total_cores as f64;
        self.max_memory_utilization = if total_memory > 0 {
            self.max_used_memory as f64 / total_memory as f64
        } else {
            1.
        };
        self.cpu_utilization_used = self.cpu_utilization / time / total_cores_used as f64;
        self.memory_utilization_used = if total_memory_used > 0 {
            self.memory_utilization / time / total_memory_used as f64
        } else {
            1.
        };
        self.cpu_utilization_active = self.cpu_utilization / total_cores_active;
        self.memory_utilization_active = if total_memory_active > 0. {
            self.memory_utilization / total_memory_active
        } else {
            1.
        };
        self.cpu_utilization /= time * total_cores as f64;
        if total_memory > 0 {
            self.memory_utilization /= time * total_memory as f64;
        } else {
            self.memory_utilization = 1.;
        }
        self.used_resource_count = self.used_resources.len();
    }
}
