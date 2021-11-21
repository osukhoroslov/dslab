use sugars::{refcell, rc};

use core::match_event;
use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext, Event};

use rand::Rng;

#[derive(Copy, Clone, Debug)]
pub struct VirtualMachine {
    id: u64,
    cpu_usage: u64,
    ram_usage: u64,
    lifetime: f64,
}

// VM EVENTS ///////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VMStart {
    actor_id: ActorId,
    vm: VirtualMachine,
}

#[derive(Debug)]
pub struct VMFinish {
    vm: VirtualMachine,
}

// CLOUD PRIMITIVES ////////////////////////////////////////////////////////////

impl VirtualMachine {
    pub fn new(cpu: u64, ram: u64, lifetime: f64, id: Option<u64>) -> Self {
        let mut rng = rand::thread_rng();

        Self {
            id: id.unwrap_or(rng.gen::<u64>()),
            cpu_usage: cpu,
            ram_usage: ram,
            lifetime: lifetime,
        }
    }
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: &ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            VMStart { actor_id, vm } => {
                println!("[time = {}]   successfully pack vm №{}",
                         ctx.time(),
                         vm.id);
                ctx.emit(VMFinish { vm: *vm },
                         actor_id,
                         vm.lifetime);
            },
            VMFinish { vm } => {
                println!("[time = {}]   vm №{} stopped with code 0",
                         ctx.time(),
                         vm.id
                );
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct Host<'a> {
    id: u64,

    cpu_full: u64,
    cpu_available: u64,

    ram_full: u64,
    ram_available: u64,

    vm_s: Vec<VirtualMachine>,
    simulation: &'a mut Simulation,
}

impl<'a, 'b> Host<'a> {
    pub fn new(simulation: &'a mut Simulation,
               cpu_full: u64,
               ram_full: u64,
               id: Option<u64>) -> Self {
        let mut rng = rand::thread_rng();

        Self {
            id: id.unwrap_or(rng.gen::<u64>()),
            cpu_full: cpu_full,
            ram_full: ram_full,
            cpu_available: cpu_full,
            ram_available: ram_full,
            vm_s: Vec::new(),
            simulation: simulation
        }
    }

    fn can_pack(&self, vm: &VirtualMachine) -> bool {
        if self.cpu_available < vm.cpu_usage {
            println!(
                "failed to assign vm {} to host {} - not enough CPU",
                vm.id,
                self.id
            );
            return false;
        }
        if self.ram_available < vm.ram_usage {
            println!(
                "failed to assign vm {} to host {} - not enough RAM",
                vm.id,
                self.id
            );
            return false;
        }
        return true;
    }

    pub fn assign_vm(&'b mut self, vm: VirtualMachine) -> bool {
        if !self.can_pack(&vm) {
            return false;
        }
        self.vm_s.push(vm);
        
        self.cpu_available -= vm.cpu_usage;
        self.ram_available -= vm.ram_usage;

        let vm_actor = self.simulation.add_actor(
            /*actor ID*/ &("VM_".to_owned() + &vm.id.to_string()), 
            /*actor entity*/ rc!(refcell!(vm))
        );
        self.simulation.add_event(
            VMStart {
                actor_id: vm_actor.clone(),
                vm: vm,
            },
            &ActorId::from("app"),
            &vm_actor,
            0.
        );
        return true;
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////

fn main() {
    let mut simulation = Simulation::new(123);
    let mut host = Host::new(&mut simulation, 100, 100, None);

    for i in 0..10 {
        let vm = VirtualMachine::new(11, 11, f64::from(i), None);
        host.assign_vm(vm);
    }

    simulation.step_until_no_events();
}