//! VM information and actions interface.

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

/// API to access virtual machines.
pub struct VmAPI {
    vms: HashMap<u32, Rc<RefCell<VirtualMachine>>>,
    vm_status: HashMap<u32, VmStatus>,
    vm_location: HashMap<u32, u32>,
    vm_counter: u32,
    ctx: SimulationContext,
}

impl VmAPI {
    /// Create component.
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            vms: HashMap::new(),
            vm_status: HashMap::new(),
            vm_location: HashMap::new(),
            vm_counter: 1,
            ctx,
        }
    }

    /// Get component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Add new VM to database. Called when VM is allacated.
    pub fn register_new_vm(&mut self, vm: VirtualMachine) {
        self.vm_status.insert(vm.id, VmStatus::Initializing);
        self.vms.insert(vm.id, rc!(refcell!(vm)));
    }

    /// Update status.
    fn update_vm_status(&mut self, vm_id: u32, status: VmStatus, host_id: u32) {
        if status == VmStatus::Running {
            self.vm_location.insert(vm_id, host_id);
        }
        self.vm_status.insert(vm_id, status);
    }

    /// Get VM status. Due to asynchronous nature of simulator there is some delay.
    pub fn get_vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_status.get(&vm_id).unwrap().clone()
    }

    /// Get reference on VM.
    pub fn get_vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        self.vms.get(&vm_id).unwrap().clone()
    }

    /// Get VM capacities in order to some process actions.
    pub fn get_vm_allocation(&self, vm_id: u32) -> Allocation {
        Allocation {
            id: vm_id,
            cpu_usage: self.vms.get(&vm_id).unwrap().borrow().cpu_usage,
            memory_usage: self.vms.get(&vm_id).unwrap().borrow().memory_usage,
        }
    }

    /// Get virtual machine location host ID.
    pub fn find_host_by_vm(&self, vm_id: u32) -> u32 {
        self.vm_location[&vm_id]
    }

    /// Generate new VM ID if user did not pass any.
    pub fn generate_vm_id(&mut self) -> u32 {
        self.vm_counter += 1;
        self.vm_counter
    }
}

impl EventHandler for VmAPI {
    /// Process VM status change event.
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            VmStatusChanged { vm_id, status } => {
                self.update_vm_status(vm_id, status, event.src);
            }
        })
    }
}
