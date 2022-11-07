//! Component that provides information about all VMs in the system.

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

/// API to access information about virtual machines.
///
/// This component stores the information about all VMs in the system, including VM characteristics, current status and
/// location, and provides an access to this information to other components. A user can also query VM API to obtain
/// the needed VM information.
pub struct VmAPI {
    vms: HashMap<u32, Rc<RefCell<VirtualMachine>>>,
    vm_status: HashMap<u32, VmStatus>,
    vm_location: HashMap<u32, u32>,
    vm_counter: u32,
    ctx: SimulationContext,
}

impl VmAPI {
    /// Creates component.
    pub fn new(ctx: SimulationContext) -> Self {
        Self {
            vms: HashMap::new(),
            vm_status: HashMap::new(),
            vm_location: HashMap::new(),
            vm_counter: 0,
            ctx,
        }
    }

    /// Returns component ID.
    pub fn get_id(&self) -> u32 {
        self.ctx.id()
    }

    /// Registers information about new VM. Called when VM is created via `CloudSimulation`.
    pub fn register_new_vm(&mut self, vm: VirtualMachine) {
        self.vm_status.insert(vm.id, VmStatus::Initializing);
        self.vms.insert(vm.id, rc!(refcell!(vm)));
    }

    /// Updates VM status and location. Called upon receiving update from host manager.
    fn update_vm_status(&mut self, vm_id: u32, status: VmStatus, host_id: u32) {
        if status == VmStatus::Running {
            self.vm_location.insert(vm_id, host_id);
        }
        self.vm_status.insert(vm_id, status);
    }

    /// Returns the status of specified VM.
    ///
    /// Due to the asynchronous propagation of status updates, the returned status may be outdated.
    pub fn get_vm_status(&self, vm_id: u32) -> VmStatus {
        self.vm_status.get(&vm_id).unwrap().clone()
    }

    /// Returns the reference to VM information by VM ID.
    pub fn get_vm(&self, vm_id: u32) -> Rc<RefCell<VirtualMachine>> {
        self.vms.get(&vm_id).unwrap().clone()
    }

    /// Returns resource allocation for specified VM.
    pub fn get_vm_allocation(&self, vm_id: u32) -> Allocation {
        Allocation {
            id: vm_id,
            cpu_usage: self.vms.get(&vm_id).unwrap().borrow().cpu_usage,
            memory_usage: self.vms.get(&vm_id).unwrap().borrow().memory_usage,
        }
    }

    // Returns the ID of host that runs the specified VM.
    pub fn find_host_by_vm(&self, vm_id: u32) -> u32 {
        self.vm_location[&vm_id]
    }

    /// Generates new VM ID if user did not pass any.
    pub fn generate_vm_id(&mut self) -> u32 {
        self.vm_counter += 1;
        self.vm_counter
    }
}

impl EventHandler for VmAPI {
    /// Processes VM status change event emitted by host manager.
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            VmStatusChanged { vm_id, status } => {
                self.update_vm_status(vm_id, status, event.src);
            }
        })
    }
}
