use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use sugars::{rc, refcell};

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::core::common::Allocation;
use crate::core::events::vm_api::VmStatusChanged;
use crate::core::vm::{VirtualMachine, VmStatus};

pub struct VmAPI {
    id: u32,
    vms: HashMap<u32, Rc<RefCell<VirtualMachine>>>,
    vm_status: HashMap<u32, VmStatus>,
    vm_location: HashMap<u32, u32>,
    vm_counter: u32,
    #[allow(dead_code)]
    ctx: SimulationContext,
}

impl VmAPI {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            id: 0,
            vms: HashMap::new(),
            vm_status: HashMap::new(),
            vm_location: HashMap::new(),
            vm_counter: 1,
            ctx: ctx,
        }
    }

    pub fn set_id(&mut self, id: u32) {
        self.id = id;
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn register_new_vm(&mut self, vm: VirtualMachine) {
        self.vm_status.insert(vm.id, VmStatus::Initializing);
        self.vms.insert(vm.id, rc!(refcell!(vm.clone())));
    }

    fn update_vm_status(&mut self, vm_id: u32, status: VmStatus, host_id: u32) {
        if status == VmStatus::Running {
            self.vm_location.insert(vm_id, host_id);
        }
        self.vm_status.insert(vm_id, status);
    }

    pub fn get_vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_status.get(&vm_id).unwrap().clone()
    }

    pub fn get_vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        self.vms.get(&vm_id).unwrap().clone()
    }

    pub fn get_vm_allocation(&self, vm_id: u32) -> Allocation {
        Allocation {
            id: vm_id,
            cpu_usage: self.vms.get(&vm_id).unwrap().borrow().cpu_usage,
            memory_usage: self.vms.get(&vm_id).unwrap().borrow().memory_usage,
        }
    }

    pub fn find_host_by_vm(&self, vm_id: u32) -> u32 {
        self.vm_location[&vm_id]
    }

    pub fn generate_vm_id(&mut self) -> u32 {
        self.vm_counter += 1;
        self.vm_counter
    }
}

impl EventHandler for VmAPI {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            VmStatusChanged { vm_id, status } => {
                self.update_vm_status(vm_id, status, event.src);
            }
        })
    }
}
