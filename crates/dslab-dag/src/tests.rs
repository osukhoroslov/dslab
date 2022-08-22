use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::CoresDependency;

use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;

use crate::dag::DAG;
use crate::dag_simulation::DagSimulation;
use crate::runner::{Config, DataTransferMode};
use crate::schedulers::heft::HeftScheduler;
use crate::schedulers::simple_scheduler::SimpleScheduler;

const PRECISION: f64 = 1. / ((1 << 20) as f64);

fn gen_dag(rng: &mut Pcg64, num_tasks: usize, num_data_items: usize) -> DAG {
    let mut dag = DAG::new();

    for i in 0..num_tasks {
        dag.add_task(
            &i.to_string(),
            rng.gen_range(1..1_000_000_000),
            rng.gen_range(0..128),
            1,
            rng.gen_range(1..6),
            match rng.gen_range(0..2) {
                0 => CoresDependency::Linear,
                1 => CoresDependency::LinearWithFixed {
                    fixed_part: rng.gen_range(0.2..0.8),
                },
                _ => CoresDependency::Linear,
            },
        );
    }

    let mut tasks_topsort: Vec<usize> = (0..num_tasks).collect();
    for i in 0..num_tasks {
        tasks_topsort.swap(i, rng.gen_range(0..i + 1));
    }

    for i in 0..num_data_items {
        let num_participants = rng.gen_range(2..4);
        let mut participants: Vec<usize> = Vec::new();
        for _ in 0..num_participants {
            participants.push(rng.gen_range(0..num_tasks));
        }
        participants.sort();
        participants.dedup();
        for task_id in participants.iter_mut() {
            *task_id = tasks_topsort[*task_id];
        }

        let size = rng.gen_range(1..1_000_000);

        // rarely generate inputs
        if rng.gen_range(0..100) == 0 {
            let id = dag.add_data_item(&i.to_string(), size);
            for task in participants.into_iter() {
                dag.add_data_dependency(id, task);
            }
        } else {
            let id = dag.add_task_output(participants[0], &i.to_string(), size);
            for task in participants.into_iter().skip(1) {
                dag.add_data_dependency(id, task);
            }
        }
    }

    dag
}

fn gen_resources(rng: &mut Pcg64, sim: &mut DagSimulation, num_resources: usize, infinite_memory: bool) {
    for i in 0..num_resources {
        sim.add_resource(
            &i.to_string(),
            rng.gen_range(1..1_000_000_000),
            rng.gen_range(1..10),
            if infinite_memory {
                1_u64 << 60
            } else {
                rng.gen_range(32..1024)
            },
        );
    }
}

fn gen_network(rng: &mut Pcg64) -> Rc<RefCell<dyn NetworkModel>> {
    Rc::new(RefCell::new(ConstantBandwidthNetwork::new(
        rng.gen_range(0.0..1_000_000.0),
        rng.gen_range(0.0..1.0),
    )))
}

#[test]
fn simple_test() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 10, 20);

    let mut sim = DagSimulation::new(
        123,
        gen_network(&mut rng),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    gen_resources(&mut rng, &mut sim, 3, false);
    sim.init(dag);
    sim.step_until_no_events();

    let result = (sim.time() / PRECISION).round() * PRECISION;
    println!("{:.100}", result);
    assert_eq!(result, 3742.044769287109375);
}

#[test]
fn test_1() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_network(&mut rng),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    gen_resources(&mut rng, &mut sim, 10, false);
    sim.init(dag);
    sim.step_until_no_events();

    let result = (sim.time() / PRECISION).round() * PRECISION;
    println!("{:.100}", result);
    assert_eq!(result, 87.13579273223876953125);
}

#[test]
fn test_2() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_network(&mut rng),
        Rc::new(RefCell::new(HeftScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    gen_resources(&mut rng, &mut sim, 10, true);
    sim.init(dag);
    sim.step_until_no_events();

    let result = (sim.time() / PRECISION).round() * PRECISION;
    println!("{:.100}", result);
    assert_eq!(result, 47.180736541748046875);
}

#[test]
fn test_3() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_network(&mut rng),
        Rc::new(RefCell::new(HeftScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
        },
    );
    gen_resources(&mut rng, &mut sim, 10, true);
    sim.init(dag);
    sim.step_until_no_events();

    let result = (sim.time() / PRECISION).round() * PRECISION;
    println!("{:.100}", result);
    assert_eq!(result, 82.22796154022216796875);
}
