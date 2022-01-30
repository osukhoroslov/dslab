use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use std::collections::btree_map::Keys;

use crate::host::AllocationVerdict;
use crate::monitoring::HostState;
use crate::monitoring::Monitoring;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub cpu_available: i64,
    pub memory_available: i64,

    pub cpu_total: u32,
    pub memory_total: u32,

    pub vms: BTreeMap<String, VirtualMachine>,
}

impl HostInfo {
    pub fn new(cpu_available: u32, memory_available: u32, cpu_total: u32, memory_total: u32) -> Self {
        Self {
            cpu_available: cpu_available.into(),
            memory_available: memory_available.into(),
            cpu_total,
            memory_total,
            vms: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    hosts: BTreeMap<String, HostInfo>,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl Store {
    pub fn new(monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            hosts: BTreeMap::new(),
            monitoring: monitoring.clone(),
        }
    }

    pub fn add_host(&mut self, id: String, state: &HostState) {
        self.hosts.insert(
            id,
            HostInfo::new(
                state.cpu_available,
                state.memory_available,
                state.cpu_total,
                state.memory_total,
            ),
        );
    }

    pub fn get_hosts_list(&self) -> Keys<String, HostInfo> {
        self.hosts.keys()
    }

    pub fn can_allocate(&mut self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
        if !self.hosts.contains_key(host_id) {
            return AllocationVerdict::HostNotFound;
        }
        if self.hosts[host_id].cpu_available < i64::from(vm.cpu_usage) {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.hosts[host_id].memory_available < i64::from(vm.memory_usage) {
            return AllocationVerdict::NotEnoughRAM;
        }
        return AllocationVerdict::Success;
    }

    pub fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available -= i64::from(vm.cpu_usage);
            host.memory_available -= i64::from(vm.memory_usage);
            host.vms.insert(vm.id.clone(), vm.clone());
        });
    }

    pub fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available += i64::from(vm.cpu_usage);
            host.memory_available += i64::from(vm.memory_usage);
            host.vms.remove(&vm.id);
        });
    }
}
