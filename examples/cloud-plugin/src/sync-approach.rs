use sugars::{rc, refcell};

use core::match_event;
use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext, Event};

use std::rc::Rc;
use std::cell::RefCell;

#[derive(Clone, Debug)]
pub struct VirtualMachine {
    id: String,
    cpu_usage: i64,
    ram_usage: i64,
    lifetime: f64,
}

#[derive(Debug, Clone)]
pub struct EnergyManager {
    energy_consumed: f64,
    prev_milestone: f64
}

#[derive(Debug, Clone)]
pub struct Host {
    id: String,

    cpu_full: i64,
    cpu_available: i64,

    ram_full: i64,
    ram_available: i64,

    vm_s: Vec<VirtualMachine>,
    vm_counter: u64,

    energy_manager: EnergyManager
}

pub struct CloudSim {
    simulation: Simulation,
    hosts: Vec<Rc<RefCell<Host>>>
}

// VM EVENTS ///////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VMStart<> {
    actor_id: ActorId,
    vm: VirtualMachine,
    host: Rc<RefCell<Host>>
}

#[derive(Debug)]
pub struct VMAllocationFailed<> {
    reason: String
}

#[derive(Debug)]
pub struct VMFinish {
    vm: VirtualMachine,
    host: Rc<RefCell<Host>>
}

// CLOUD PRIMITIVES ////////////////////////////////////////////////////////////

impl VirtualMachine {
    pub fn new(cpu: i64, ram: i64, lifetime: f64) -> Self {
        Self {
            id: "".to_string(),
            cpu_usage: cpu,
            ram_usage: ram,
            lifetime: lifetime,
        }
    }
}

impl Actor for VirtualMachine {
    fn on(&mut self, event: Box<dyn Event>, 
                     _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            VMStart { actor_id, vm, host } => {
                println!("[time = {}] successfully pack vm #{}",
                         ctx.time(),
                         vm.id);
                host.borrow_mut().recalculate_energy(ctx.time());
                ctx.emit(VMFinish { vm: vm.clone(), host: Rc::clone(host) },
                         actor_id.clone(),
                         vm.lifetime);
            },
            VMAllocationFailed { reason } => {
                println!("[time = {}] {}", ctx.time(), reason);
            },
            VMFinish { vm, host } => {
                host.borrow_mut().recalculate_energy(ctx.time());
                host.borrow_mut().free_vm(vm);

                println!("[time = {}] vm #{} stopped with code 0",
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

impl Host {
    pub fn new(cpu_full: i64, ram_full: i64, id: String) -> Self {
        Self {
            id: id,
            cpu_full: cpu_full,
            ram_full: ram_full,
            cpu_available: cpu_full,
            ram_available: ram_full,
            vm_s: Vec::new(),
            vm_counter: 0,
            energy_manager: EnergyManager::new()
        }
    }

    fn find_vm_index(&self, id: String) -> Option<usize> {
        for i in 0..self.vm_s.len() {
            if self.vm_s[i].id == id {
               return Some(i);
            }
        }
        return None;
    }

    fn can_pack(&self, vm: &VirtualMachine) -> Option<String> {
        if self.cpu_available < vm.cpu_usage {
            let error = format!("failed to assign vm to host {} - not enough CPU", vm.id);
            return Some(error);
        }
        if self.ram_available < vm.ram_usage {
            let error = format!("failed to assign vm to host {} - not enough RAM", vm.id);
            return Some(error);
        }
        return None;
    }

    pub fn assign_vm(&mut self, vm: &mut VirtualMachine) -> Option<String> {
        let error = self.can_pack(&vm);
        if error != None {
            return error;
        }

        vm.id = self.id.to_string() + &"/".to_string() + &self.vm_counter.to_string();
        self.vm_counter += 1;
        self.vm_s.push(vm.clone());
        
        self.cpu_available -= vm.cpu_usage;
        self.ram_available -= vm.ram_usage;

        return None;
    }

    pub fn free_vm(&mut self, vm: &VirtualMachine) {
        self.cpu_available += vm.cpu_usage;
        self.ram_available += vm.ram_usage;

        let vm_index = self.find_vm_index(vm.id.clone()).unwrap();
        self.vm_s.swap_remove(vm_index);
    }

    pub fn cpu_load(&self) -> f64 {
        return 1.0 - self.cpu_available as f64 / self.cpu_full as f64;
    }

    pub fn ram_load(&self) -> f64 {
        return 1.0 - self.ram_available as f64 / self.ram_full as f64;
    }

    pub fn recalculate_energy(&mut self, time: f64) {
        self.energy_manager.energy_consumed +=
                (0.4 + 0.6 * self.cpu_load()) *
                (time - self.energy_manager.prev_milestone);
        self.energy_manager.prev_milestone = time;
    }
}

impl EnergyManager {
    pub fn new() -> Self {
        Self {
            prev_milestone: 0.0,
            energy_consumed: 0.0
        }
    }
}

impl CloudSim {
    pub fn new(seed: u64) -> Self {
        Self {
            simulation: Simulation::new(seed),
            hosts: Vec::new()
        }
    }

    pub fn spawn_host(&mut self, cpu_full: i64, ram_full: i64) -> String {
        let current_num = self.hosts.len().to_string();
        self.hosts.push(rc!(refcell!(Host::new(cpu_full, ram_full, current_num.clone()))));
        return current_num;
    }

    pub fn spawn_vm(&mut self, host_id: String, cpu: i64, ram: i64, lifetime: f64)
    -> Option<String> {
        let host_id_: usize = host_id.parse().unwrap();
        let mut vm = VirtualMachine::new(cpu, ram, lifetime);
        let error = self.hosts[host_id_].borrow_mut().assign_vm(&mut vm);

        let vm_actor = self.simulation.add_actor(
            /*actor ID*/ &("VM_".to_owned() + &vm.id.clone()), 
            /*actor entity*/ rc!(refcell!(vm.clone()))
        );

        if error != None {
            self.simulation.add_event(
                VMAllocationFailed { 
                    reason: error.unwrap()
                },
                ActorId::from("app"),
                vm_actor,
                0.
            );
            return None;
        }

        self.simulation.add_event(
            VMStart {
                host: self.hosts[host_id_].clone(),
                actor_id: vm_actor.clone(),
                vm: vm.clone(),
            },
            ActorId::from("app"),
            vm_actor,
            0.
        );
        return Some(vm.id);
    }

    pub fn get_cpu_load_by_host_id(&self, host_id: String) -> f64 {
        let host_id_: usize = host_id.parse().unwrap();

        let available = self.hosts[host_id_].borrow_mut().cpu_available;
        let max = self.hosts[host_id_].borrow_mut().cpu_full;
        return 1.0 - available as f64 / max as f64;
    }

    pub fn get_ram_load_by_host_id(&self, host_id: String) -> f64 {
        let host_id_: usize = host_id.parse().unwrap();
        return self.hosts[host_id_].borrow_mut().cpu_load();
    }

    pub fn get_energy_consumption_by_host_id(&self, host_id: String) -> f64 {
        let host_id_: usize = host_id.parse().unwrap();
        self.hosts[host_id_].borrow_mut().recalculate_energy(self.simulation.time());
        return self.hosts[host_id_].borrow_mut().energy_manager.energy_consumed;
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////

/*
fn main() {
    let mut cloud_sim = CloudSim::new(123);
    let host_one = cloud_sim.spawn_host(100, 100);
    let _host_two = cloud_sim.spawn_host(100, 100);

    for i in 0..10 {
        let _vm_id = cloud_sim.spawn_vm(host_one.clone(), 11, 11, f64::from(i));
    }

    for _i in 0..9 {
        println!("[time = {}] Host CPU load is {}, overall consumption is {}",
                 cloud_sim.simulation.time(),
                 cloud_sim.get_cpu_load_by_host_id(host_one.clone()),
                 cloud_sim.get_energy_consumption_by_host_id(host_one.clone())
        );
        cloud_sim.simulation.step_for_duration(1.0);
    }
}
*/
