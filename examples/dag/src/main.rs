mod simple_scheduler;

use sugars::{rc, refcell};

use compute::multicore::*;
use core::simulation::Simulation;
use dag::dag::DAG;
use dag::runner::*;
use network::constant_bandwidth_model::ConstantBandwidthNetwork;
use network::network::Network;

use crate::simple_scheduler::SimpleScheduler;

fn map_reduce() {
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

    let mut resources: Vec<Resource> = Vec::new();
    let mut add_resource = |speed: u64, cores: u32, memory: u64| {
        let compute_id = format!("compute{}", resources.len() + 1);
        let compute = rc!(refcell!(Compute::new(
            speed,
            cores,
            memory,
            sim.create_context(&compute_id)
        )));
        sim.add_handler(&compute_id, compute.clone());
        let resource = Resource {
            id: compute_id,
            compute,
            speed,
            cores_available: cores,
            memory_available: memory,
        };
        resources.push(resource);
    };
    add_resource(10, 2, 256);
    add_resource(20, 1, 512);
    add_resource(30, 4, 1024);

    let network_model = rc!(refcell!(ConstantBandwidthNetwork::new(10.0, 0.1)));
    let network = rc!(refcell!(Network::new(network_model, sim.create_context("net"))));
    sim.add_handler("net", network.clone());

    let scheduler = SimpleScheduler::new();
    let runner_id = "runner";
    let runner = rc!(refcell!(DAGRunner::new(
        dag,
        network,
        resources,
        scheduler,
        sim.create_context(runner_id)
    )));
    sim.add_handler(runner_id, runner.clone());

    let mut client = sim.create_context("client");
    client.emit_now(Start {}, runner_id);
    sim.step_until_no_events();
    runner
        .borrow()
        .trace_log()
        .save_to_file("trace_map_reduce.json")
        .unwrap();
}

fn epigenomics() {
    let dag = DAG::from_dax("Epigenomics_100.xml");

    let mut sim = Simulation::new(123);

    let mut resources: Vec<Resource> = Vec::new();
    let mut add_resource = |speed: u64, cores: u32, memory: u64| {
        let name = format!("compute{}", resources.len() + 1);
        let compute = Rc::new(RefCell::new(Compute::new(&name, speed, cores, memory)));
        sim.add_actor(&name, compute.clone());
        let resource = Resource {
            compute,
            id: ActorId::from(&name),
            speed,
            cores_available: cores,
            memory_available: memory,
        };
        resources.push(resource);
    };
    add_resource(10, 8, 256);
    add_resource(20, 2, 512);
    add_resource(30, 4, 1024);

    let network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(100000.0, 10.)));
    let network = Rc::new(RefCell::new(Network::new(network_model)));
    sim.add_actor(NETWORK_ID, network.clone());

    let scheduler = SimpleScheduler::new();
    let runner = rc!(refcell!(DAGRunner::new(dag, network, resources, scheduler)));
    let runner_id = sim.add_actor("runner", runner.clone());
    sim.add_event_now(Start {}, ActorId::from("client"), runner_id);
    sim.step_until_no_events();
    runner
        .borrow()
        .trace_log()
        .save_to_file("trace_epigenomics.json")
        .unwrap();
}

fn montage() {
    let dag = DAG::from_dot("Montage.dot");

    let mut sim = Simulation::new(123);

    let mut resources: Vec<Resource> = Vec::new();
    let mut add_resource = |speed: u64, cores: u32, memory: u64| {
        let name = format!("compute{}", resources.len() + 1);
        let compute = Rc::new(RefCell::new(Compute::new(&name, speed, cores, memory)));
        sim.add_actor(&name, compute.clone());
        let resource = Resource {
            compute,
            id: ActorId::from(&name),
            speed,
            cores_available: cores,
            memory_available: memory,
        };
        resources.push(resource);
    };
    add_resource(10, 8, 256);
    add_resource(20, 2, 512);
    add_resource(30, 4, 1024);

    let network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(0.01, 1.)));
    let network = Rc::new(RefCell::new(Network::new(network_model)));
    sim.add_actor(NETWORK_ID, network.clone());

    let scheduler = SimpleScheduler::new();
    let runner = rc!(refcell!(DAGRunner::new(dag, network, resources, scheduler)));
    let runner_id = sim.add_actor("runner", runner.clone());
    sim.add_event_now(Start {}, ActorId::from("client"), runner_id);
    sim.step_until_no_events();
    runner.borrow().trace_log().save_to_file("trace_montage.json").unwrap();
}

fn diamond() {
    let dag = DAG::from_yaml("diamond.yaml");

    let mut sim = Simulation::new(123);

    let mut resources: Vec<Resource> = Vec::new();
    let mut add_resource = |speed: u64, cores: u32, memory: u64| {
        let name = format!("compute{}", resources.len() + 1);
        let compute = Rc::new(RefCell::new(Compute::new(&name, speed, cores, memory)));
        sim.add_actor(&name, compute.clone());
        let resource = Resource {
            compute,
            id: ActorId::from(&name),
            speed,
            cores_available: cores,
            memory_available: memory,
        };
        resources.push(resource);
    };
    add_resource(10, 1, 256);
    add_resource(20, 3, 512);

    let network_model = Rc::new(RefCell::new(ConstantBandwidthNetwork::new(100., 0.1)));
    let network = Rc::new(RefCell::new(Network::new(network_model)));
    sim.add_actor(NETWORK_ID, network.clone());

    let scheduler = SimpleScheduler::new();
    let runner = rc!(refcell!(DAGRunner::new(dag, network, resources, scheduler)));
    let runner_id = sim.add_actor("runner", runner.clone());
    sim.add_event_now(Start {}, ActorId::from("client"), runner_id);
    sim.step_until_no_events();
    runner.borrow().trace_log().save_to_file("trace_diamond.json").unwrap();
}

fn main() {
    map_reduce();
    epigenomics();  // dax
    montage();  // dot
    diamond(); // yaml
}
