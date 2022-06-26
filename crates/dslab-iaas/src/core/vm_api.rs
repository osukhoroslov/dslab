use std::collections::HashMap;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::core::events::vm_api::VmStatusChanged;
use crate::core::vm::{VirtualMachine, VmStatus};

pub struct VmAPI {
    vms: HashMap<u32, VirtualMachine>,
    vm_status: HashMap<u32, VmStatus>,
    _ctx: SimulationContext,
}

impl VmAPI {
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            vms: HashMap::new(),
            vm_status: HashMap::new(),
            _ctx: ctx,
        }
    }

    pub fn register_new_vm(&mut self, vm: VirtualMachine) {
        self.vms.insert(vm.id, vm.clone());
        self.vm_status.insert(vm.id, VmStatus::Initializing);
    }

    fn update_vm_status(&mut self, vm_id: u32, status: VmStatus) {
        self.vm_status.insert(vm_id, status);
    }

    pub fn get_vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_status.get(&vm_id).unwrap().clone()
    }

    pub fn get_vm(&self, vm_id: u32) -> VirtualMachine {
        self.vms.get(&vm_id).unwrap().clone()
    }
}

impl EventHandler for VmAPI {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            VmStatusChanged { vm_id, status } => {
                self.update_vm_status(vm_id, status);
            }
        })
    }
}
