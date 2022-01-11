mod dag;
mod runner;

use crate::dag::*;
use crate::runner::*;
use core::actor::ActorId;
use core::sim::Simulation;
use sugars::{rc, refcell};

fn main() {
    let mut dag = DAG::new();

    let data_part1 = dag.add_data_item("part1", 128);
    let data_part2 = dag.add_data_item("part2", 64);

    let map1 = dag.add_task("map1", 100);
    dag.add_data_dependency(data_part1, map1);
    let map1_out1 = dag.add_task_output(map1, "out1", 10);
    let map1_out2 = dag.add_task_output(map1, "out2", 10);

    let map2 = dag.add_task("map2", 120);
    dag.add_data_dependency(data_part2, map2);
    let map2_out1 = dag.add_task_output(map2, "out1", 10);
    let map2_out2 = dag.add_task_output(map2, "out2", 10);

    let reduce1 = dag.add_task("reduce1", 60);
    dag.add_data_dependency(map1_out1, reduce1);
    dag.add_data_dependency(map2_out1, reduce1);

    let reduce2 = dag.add_task("reduce2", 50);
    dag.add_data_dependency(map1_out2, reduce2);
    dag.add_data_dependency(map2_out2, reduce2);

    let mut sim = Simulation::new(123);
    let runner = sim.add_actor("runner", rc!(refcell!(DAGRunner::new(dag))));
    sim.add_event_now(Start {}, ActorId::from("client"), runner);
    sim.step_until_no_events();
}
