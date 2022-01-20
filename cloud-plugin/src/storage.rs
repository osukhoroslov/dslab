use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use core::actor::ActorId;

use crate::host::AllocationVerdict;
use crate::monitoring::Monitoring;
use crate::virtual_machine::VirtualMachine;

#[derive(Debug, Clone)]
pub struct HostInfo {
    pub cpu_available: i64,
    pub ram_available: i64,

    pub cpu_full: u32,
    pub ram_full: u32,

    pub vms: BTreeMap<String, VirtualMachine>,
}

impl HostInfo {
    pub fn new(cpu_full: u32, ram_full: u32) -> Self {
        Self {
            cpu_available: i64::from(cpu_full),
            ram_available: i64::from(ram_full),
            cpu_full,
            ram_full,
            vms: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    hosts: BTreeMap<String, HostInfo>,
    monitoring: Rc<RefCell<Monitoring>>,
}

impl Storage {
    pub fn new(monitoring: Rc<RefCell<Monitoring>>) -> Self {
        Self {
            hosts: BTreeMap::new(),
            monitoring: monitoring.clone()
        }
    }

    pub fn add_host(&mut self, id: String, cpu_full: u32, ram_full: u32) {
        self.hosts.insert(id, HostInfo::new(cpu_full, ram_full));
    }

    pub fn can_allocate(&mut self, vm: &VirtualMachine, host_id: &String) -> AllocationVerdict {
        if !self.hosts.contains_key(host_id) {
            let state = self.monitoring.borrow().get_host_state(ActorId::from(host_id));
            self.hosts.insert(host_id.to_string(), HostInfo::new(state.cpu_full, state.ram_full));
        }

        if self.hosts[host_id].cpu_available < i64::from(vm.cpu_usage) {
            return AllocationVerdict::NotEnoughCPU;
        }
        if self.hosts[host_id].ram_available < i64::from(vm.ram_usage) {
            return AllocationVerdict::NotEnoughRAM;
        }
        return AllocationVerdict::Success;
    }

    pub fn place_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available -= i64::from(vm.cpu_usage);
            host.ram_available -= i64::from(vm.ram_usage);
            host.vms.insert(vm.id.clone(), vm.clone());
        });
    }

    pub fn remove_vm(&mut self, vm: &VirtualMachine, host_id: &String) {
        self.hosts.get_mut(host_id).map(|host| {
            host.cpu_available += i64::from(vm.cpu_usage);
            host.ram_available += i64::from(vm.ram_usage);
            host.vms.remove(&vm.id);
        });
    }
}
