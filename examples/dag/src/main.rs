mod dag;
mod runner;
mod trace_log;

use std::cell::RefCell;
use std::rc::Rc;

use crate::dag::*;
use crate::runner::*;
use core::actor::ActorId;
use core::sim::Simulation;
use sugars::{rc, refcell};

use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network_actor::{Network, NETWORK_ID};

use compute::multicore::*;

fn main() {
    let mut dag = DAG::new();

    let data_part1 = dag.add_data_item("part1", 128);
    let data_part2 = dag.add_data_item("part2", 64);

    let map1 = dag.add_task("map1", 100, 512, 1, 2, CoresDependency::Linear);
    dag.add_data_dependency(data_part1, map1);
    let map1_out1 = dag.add_task_output(map1, "map1_out1", 10);
    let map1_out2 = dag.add_task_output(map1, "map1_out2", 10);
    let map1_out3 = dag.add_task_output(map1, "map1_out3", 10);
    let map1_out4 = dag.add_task_output(map1, "map1_out4", 10);

    let map2 = dag.add_task("map2", 120, 512, 2, 4, CoresDependency::Linear);
    dag.add_data_dependency(data_part2, map2);
    let map2_out1 = dag.add_task_output(map2, "map2_out1", 10);
    let map2_out2 = dag.add_task_output(map2, "map2_out2", 10);
    let map2_out3 = dag.add_task_output(map2, "map2_out3", 10);
    let map2_out4 = dag.add_task_output(map2, "map2_out4", 10);

    let reduce1 = dag.add_task("reduce1", 60, 128, 2, 3, CoresDependency::Linear);
    dag.add_data_dependency(map1_out1, reduce1);
    dag.add_data_dependency(map2_out1, reduce1);

    let reduce2 = dag.add_task("reduce2", 50, 128, 1, 1, CoresDependency::Linear);
    dag.add_data_dependency(map1_out2, reduce2);
    dag.add_data_dependency(map2_out2, reduce2);

    let reduce3 = dag.add_task("reduce3", 100, 128, 1, 2, CoresDependency::Linear);
    dag.add_data_dependency(map1_out3, reduce3);
    dag.add_data_dependency(map2_out3, reduce3);

    let reduce4 = dag.add_task("reduce4", 110, 128, 1, 1, CoresDependency::Linear);
    dag.add_data_dependency(map1_out4, reduce4);
    dag.add_data_dependency(map2_out4, reduce4);

    dag.add_task_output(reduce1, "result1", 32);
    dag.add_task_output(reduce2, "result2", 32);
    dag.add_task_output(reduce3, "result3", 32);
    dag.add_task_output(reduce4, "result4", 32);

    let mut sim = Simulation::new(123);

    let mut compute_actors: Vec<Resource> = Vec::new();
    let mut add_compute = |speed: u64, cores: u32, memory: u64| {
        let name = format!("compute{}", compute_actors.len() + 1);
        let compute = Rc::new(RefCell::new(Compute::new(&name, speed, cores, memory)));
        sim.add_actor(&name, compute.clone());
        let resource = Resource {
            compute,
            id: ActorId::from(&name),
            cores_available: cores,
            memory_available: memory,
        };
        compute_actors.push(resource);
    };
    add_compute(10, 2, 256);
    add_compute(20, 1, 512);
    add_compute(30, 4, 1024);

    let constant_network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(10.0, 0.1)));
    let constant_network = Rc::new(RefCell::new(Network::new(constant_network_model)));
    sim.add_actor(NETWORK_ID, constant_network.clone());

    let runner_actor = rc!(refcell!(DAGRunner::new(dag, constant_network, compute_actors)));
    let runner = sim.add_actor("runner", runner_actor.clone());
    sim.add_event_now(Start {}, ActorId::from("client"), runner);
    sim.step_until_no_events();
    runner_actor.borrow().trace_log().save_to_file("trace.json").unwrap();
}
