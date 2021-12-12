mod virtual_machine;
mod host;
mod cloud_balancer;

use std::rc::Rc;
use std::cell::RefCell;

use core::sim::Simulation;
use core::actor::Actor;
use cloud_balancer::CloudBalancer;
use crate::host::SendMonitoringStats;
use crate::host::HostAllocationAgent;
use crate::virtual_machine::VirtualMachine;
use crate::cloud_balancer::FindHostToAllocateVM;
use sugars::{rc, refcell};
use core::actor::{ActorId};

pub fn SpawnHost(sim: &mut Simulation,
                 id: String,
                 cpu_capacity: i64,
                 ram_capacity: i64,
                 balancers: Vec<ActorId>) -> ActorId {
    let host = rc!(refcell!(HostAllocationAgent::new(cpu_capacity, ram_capacity, id.clone())));
    for balancer in balancers {
        host.borrow_mut().add_subscriber(balancer);
    }

    let actor_id = ActorId::from(&id.clone());
    let actor = sim.add_actor(&id.clone(), host.clone());

    sim.add_event(SendMonitoringStats { }, actor_id, actor.clone(), 0.); 
    return actor;
}

pub fn SpawnBalancer(sim: &mut Simulation, id: String, hosts: Vec<ActorId>) -> ActorId {
    let balancer = rc!(refcell!(CloudBalancer::new(ActorId::from(&id.clone()))));
    for host in hosts {
        balancer.borrow_mut().add_host(host);
    }

    let actor_id = ActorId::from(&id.clone());
    let actor = sim.add_actor(&id.clone(), balancer.clone());
    return actor;
}

fn main() {
    /// INIT ///////////////////////////////////////////////////////////////////////////////////////
    let mut sim = Simulation::new(123);
    let hosts = Vec::from([ActorId::from("h1"), ActorId::from("h2")]);
    let balancers = Vec::from([ActorId::from("b")]);

    let host_one = SpawnHost(&mut sim, "h1".to_string(), 30, 30, balancers.clone());
    let host_two = SpawnHost(&mut sim, "h2".to_string(), 30, 30, balancers.clone());
    let balancer = SpawnBalancer(&mut sim, "b".to_string(), hosts);

    /// SIMULATION /////////////////////////////////////////////////////////////////////////////////

    for i in 0..10 {
        let vm_name = "v".to_owned() + &i.to_string();
        let vm = VirtualMachine::new(vm_name.clone(), 10, 10, 2.0);
        sim.add_actor(&vm_name.clone(), rc!(refcell!(vm.clone())).clone());

        sim.add_event(FindHostToAllocateVM { vm }, balancer.clone(), balancer.clone(), 0.0);
    }

    for i in 0..250 {
        let _ok = sim.step();
    }
}
