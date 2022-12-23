use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::CoresDependency;

use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;

use crate::dag::DAG;
use crate::dag_simulation::DagSimulation;
use crate::data_item::DataTransferMode;
use crate::runner::Config;
use crate::schedulers::dls::DlsScheduler;
use crate::schedulers::heft::HeftScheduler;
use crate::schedulers::lookahead::LookaheadScheduler;
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
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_eq!(result, 2559.57426166534423828125);
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
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
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
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_eq!(result, 30.5806369781494140625);
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
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_eq!(result, 82.22796154022216796875);
}

#[test]
fn test_4() {
    let mut dag = DAG::new();

    dag.add_task("A", 64, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("B", 76, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("C", 52, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("D", 32, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("E", 56, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("F", 64, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("G", 60, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("H", 44, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("I", 48, 0, 1, 1, CoresDependency::Linear);
    dag.add_task("J", 28, 0, 1, 1, CoresDependency::Linear);

    let mut add_edge = |from: usize, to: usize, size: u64, name: &str| {
        let id = dag.add_task_output(from, name, size);
        dag.add_data_dependency(id, to);
    };

    add_edge(0, 1, 18, "a");
    add_edge(0, 2, 12, "b");
    add_edge(0, 3, 9, "c");
    add_edge(0, 4, 11, "d");
    add_edge(0, 5, 14, "e");
    add_edge(1, 7, 19, "f");
    add_edge(1, 8, 16, "g");
    add_edge(2, 6, 23, "h");
    add_edge(3, 7, 27, "i");
    add_edge(3, 8, 23, "j");
    add_edge(4, 8, 13, "k");
    add_edge(5, 7, 15, "l");
    add_edge(6, 9, 17, "m");
    add_edge(7, 9, 11, "n");
    add_edge(8, 9, 13, "o");

    fn run_sim(scheduler: impl crate::scheduler::Scheduler + 'static, dag: DAG) -> f64 {
        let mut sim = DagSimulation::new(
            123,
            Rc::new(RefCell::new(ConstantBandwidthNetwork::new(1.0, 0.0))),
            Rc::new(RefCell::new(scheduler)),
            Config {
                data_transfer_mode: DataTransferMode::Direct,
            },
        );
        sim.add_resource("0", 1, 1, 0);
        sim.add_resource("1", 2, 1, 0);
        sim.add_resource("2", 4, 1, 0);
        sim.add_resource("3", 4, 1, 0);

        let runner = sim.init(dag);
        sim.step_until_no_events();
        assert!(runner.borrow().is_completed());

        return sim.time();
    }
    let heft_makespan = run_sim(HeftScheduler::new(), dag.clone());
    assert_eq!(heft_makespan, 98.0);

    // 0:
    // 1:                           [-------------E------------]
    // 2:[-------A------][-----C-----][--------B--------][------G------]
    // 3:                              [-------F------][---D--]              [-----I----][----H----][--J--]

    let reverse_lookahead_makespan = run_sim(LookaheadScheduler::new(), dag.clone());
    assert_eq!(reverse_lookahead_makespan, 94.0);

    // 0:
    // 1:                         [-------D------]
    // 2:[-------A------][-----C-----][--------B--------][-------F------][-----I----][----H----][--J--]
    // 3:                           [------E-----]           [------G------]

    let reverse_lookahead_makespan = run_sim(DlsScheduler::new(), dag);
    assert_eq!(reverse_lookahead_makespan, 100.0);

    // 0:
    // 1:                           [--------------E-----------]
    // 2:[-------A------][--------B--------][-------F------]                         [----H----]      [--J--]
    // 3:                            [-----C-----][---D--][------G------]    [-----I----]
}

#[test]
fn test_chain_1() {
    let mut dag = DAG::new();
    for i in 0..10 {
        dag.add_task(&i.to_string(), i * 10 + 20, 32, 1, 2, CoresDependency::Linear);
    }
    for i in 0..9 {
        let id = dag.add_task_output(i, &i.to_string(), i as u64 * 100 + 200);
        dag.add_data_dependency(id, i + 1);
    }
    let input = dag.add_data_item("input", 600);
    dag.add_data_dependency(input, 0);
    dag.add_task_output(9, "output", 700);

    let bandwidth = 10.0;
    let latency = 0.1;

    let mut correct_result = 0.;
    for task in dag.get_tasks() {
        correct_result += task.flops as f64 / 5. / 2.;
    }
    correct_result += 600. / bandwidth + latency;
    correct_result += 700. / bandwidth + latency;

    let mut sim = DagSimulation::new(
        123,
        Rc::new(RefCell::new(ConstantBandwidthNetwork::new(bandwidth, latency))),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    sim.add_resource("0", 5, 10, 1024);
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_eq!(result, correct_result);
}

#[test]
fn test_chain_2() {
    let mut dag = DAG::new();
    for i in 0..10 {
        dag.add_task(&i.to_string(), i * 10 + 20, 32, 1, 2, CoresDependency::Linear);
    }
    for i in 0..9 {
        let id = dag.add_task_output(i, &i.to_string(), i as u64 * 100 + 200);
        dag.add_data_dependency(id, i + 1);
    }
    let input = dag.add_data_item("input", 600);
    dag.add_data_dependency(input, 0);
    dag.add_task_output(9, "output", 700);

    let bandwidth = 10.0;
    let latency = 0.1;

    let mut correct_result = 0.;
    for task in dag.get_tasks() {
        correct_result += task.flops as f64 / 5. / 2.;
    }
    for data_item in dag.get_data_items() {
        if data_item.name == "input" || data_item.name == "output" {
            continue;
        }
        correct_result += (data_item.size as f64 / bandwidth + latency) * 2.;
    }
    correct_result += 600. / bandwidth + latency;
    correct_result += 700. / bandwidth + latency;

    let mut sim = DagSimulation::new(
        123,
        Rc::new(RefCell::new(ConstantBandwidthNetwork::new(bandwidth, latency))),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
        },
    );
    sim.add_resource("0", 5, 10, 1024);
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_eq!(result, correct_result);
}

#[test]
fn test_fork_join() {
    let mut dag = DAG::new();
    let root = dag.add_task("root", 10, 32, 1, 1, CoresDependency::Linear);
    let end = dag.add_task("end", 10, 32, 1, 1, CoresDependency::Linear);
    for i in 0..5 {
        let data_id = dag.add_task_output(root, &i.to_string(), 100);
        let task_id = dag.add_task(&i.to_string(), 50, 32, 1, 1, CoresDependency::Linear);
        dag.add_data_dependency(data_id, task_id);
        let data_id = dag.add_task_output(task_id, &(i.to_string() + "_"), 200);
        dag.add_data_dependency(data_id, end);
    }

    let bandwidth = 10.0;
    let latency = 0.1;

    let mut correct_result = (10. + 50. + 10.) / 5.;
    correct_result += 100. / bandwidth + 0.1;
    correct_result += 200. / bandwidth + 0.1;

    let mut sim = DagSimulation::new(
        123,
        Rc::new(RefCell::new(ConstantBandwidthNetwork::new(bandwidth, latency))),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    for i in 0..5 {
        sim.add_resource(&i.to_string(), 5, 1, 1024);
    }
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_eq!(result, correct_result);
}
