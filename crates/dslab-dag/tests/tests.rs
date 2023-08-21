use std::cell::RefCell;
use std::rc::Rc;

use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::CoresDependency;
use dslab_core::EPSILON;

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::NetworkConfig;
use dslab_dag::resource::ResourceConfig;
use dslab_dag::runner::Config;
use dslab_dag::scheduler::Scheduler;
use dslab_dag::schedulers::dls::DlsScheduler;
use dslab_dag::schedulers::heft::HeftScheduler;
use dslab_dag::schedulers::lookahead::LookaheadScheduler;
use dslab_dag::schedulers::peft::PeftScheduler;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

const PRECISION: f64 = 1. / ((1 << 20) as f64);

fn assert_float_eq(x: f64, y: f64, eps: f64) {
    assert!(
        (x - y).abs() < eps || (x.max(y) - x.min(y)) / x.min(y) < eps,
        "Values do not match: {:.15} vs {:.15}",
        x,
        y
    );
}

fn gen_dag(rng: &mut Pcg64, num_tasks: usize, num_data_items: usize) -> DAG {
    let mut dag = DAG::new();

    for i in 0..num_tasks {
        dag.add_task(
            &i.to_string(),
            rng.gen_range::<u64, _>(1..1_000_000_000) as f64,
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

        let size = rng.gen_range::<u64, _>(1..1_000_000) as f64;

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

fn gen_resources(rng: &mut Pcg64, num_resources: usize, infinite_memory: bool) -> Vec<ResourceConfig> {
    (0..num_resources)
        .map(|i| ResourceConfig {
            name: i.to_string(),
            speed: rng.gen_range::<u64, _>(1..1_000_000_000) as f64,
            cores: rng.gen_range(1..10),
            memory: if infinite_memory {
                1_u64 << 60
            } else {
                rng.gen_range(32..1024)
            },
        })
        .collect()
}

fn gen_network(rng: &mut Pcg64) -> NetworkConfig {
    NetworkConfig::constant(rng.gen_range(0.0..1_000_000.0), rng.gen_range(0.0..1.0) * 1e6)
}

#[test]
fn simple_test() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 10, 20);

    let mut sim = DagSimulation::new(
        123,
        gen_resources(&mut rng, 3, false),
        gen_network(&mut rng),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_float_eq(result, 1183.88168430328369140625, EPSILON);
}

#[test]
fn test_1() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_resources(&mut rng, 10, false),
        gen_network(&mut rng),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_float_eq(result, 103.860325813293457, EPSILON);
}

#[test]
fn test_2() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_resources(&mut rng, 10, true),
        gen_network(&mut rng),
        Rc::new(RefCell::new(HeftScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_float_eq(result, 35.046996116638184, EPSILON);
}

#[test]
fn test_3() {
    let mut rng = Pcg64::seed_from_u64(1);
    let dag = gen_dag(&mut rng, 1000, 5000);

    let mut sim = DagSimulation::new(
        123,
        gen_resources(&mut rng, 10, true),
        gen_network(&mut rng),
        Rc::new(RefCell::new(HeftScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
        },
    );
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = (sim.time() / PRECISION).round() * PRECISION;
    assert_float_eq(result, 105.132052421569824, EPSILON);
}

#[test]
fn test_4() {
    let mut dag = DAG::new();

    dag.add_task("A", 64., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("B", 76., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("C", 52., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("D", 32., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("E", 56., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("F", 64., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("G", 60., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("H", 44., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("I", 48., 0, 1, 1, CoresDependency::Linear);
    dag.add_task("J", 28., 0, 1, 1, CoresDependency::Linear);

    let mut add_edge = |from: usize, to: usize, size: f64, name: &str| {
        let id = dag.add_task_output(from, name, size);
        dag.add_data_dependency(id, to);
    };

    add_edge(0, 1, 18., "a");
    add_edge(0, 2, 12., "b");
    add_edge(0, 3, 9., "c");
    add_edge(0, 4, 11., "d");
    add_edge(0, 5, 14., "e");
    add_edge(1, 7, 19., "f");
    add_edge(1, 8, 16., "g");
    add_edge(2, 6, 23., "h");
    add_edge(3, 7, 27., "i");
    add_edge(3, 8, 23., "j");
    add_edge(4, 8, 13., "k");
    add_edge(5, 7, 15., "l");
    add_edge(6, 9, 17., "m");
    add_edge(7, 9, 11., "n");
    add_edge(8, 9, 13., "o");

    fn run_scheduler(scheduler: impl Scheduler + 'static, dag: DAG) -> f64 {
        let mut sim = DagSimulation::new(
            123,
            Vec::new(),
            NetworkConfig::constant(1.0, 0.0),
            Rc::new(RefCell::new(scheduler)),
            Config {
                data_transfer_mode: DataTransferMode::Direct,
            },
        );
        sim.add_resource("0", 1., 1, 0);
        sim.add_resource("1", 2., 1, 0);
        sim.add_resource("2", 4., 1, 0);
        sim.add_resource("3", 4., 1, 0);

        let runner = sim.init(dag);
        runner.borrow_mut().enable_trace_log(true);
        sim.step_until_no_events();
        assert!(runner.borrow().is_completed());

        runner.borrow().trace_log().save_to_file("simple.log").unwrap();
        return sim.time();
    }

    let heft_makespan = run_scheduler(HeftScheduler::new(), dag.clone());
    // 0:
    // 1:                           [-------------E------------]
    // 2:[-------A------][-----C-----][--------B--------][------G------]
    // 3:                              [-------F------][---D--]              [-----I----][----H----][--J--]
    assert_float_eq(heft_makespan, 98.0, EPSILON);

    let lookahead_makespan = run_scheduler(LookaheadScheduler::new(), dag.clone());
    // 0:
    // 1:                         [-------D------]
    // 2:[-------A------][-----C-----][--------B--------][-------F------][-----I----][----H----][--J--]
    // 3:                           [------E-----]           [------G------]
    assert_float_eq(lookahead_makespan, 94.0, EPSILON);

    let dls_makespan = run_scheduler(DlsScheduler::new(), dag.clone());
    // 0:
    // 1:                           [--------------E-----------]
    // 2:[-------A------][-----C-----][--------B--------][------G------]     [-----I----]
    // 3:                         [---D--][-------F------]                  [----H----]           [--J--]
    assert_float_eq(dls_makespan, 100.0, EPSILON);

    let peft_makespan = run_scheduler(PeftScheduler::new().with_original_network_estimation(), dag.clone());
    // 0:
    // 1:
    // 2:[-------A------][-----C-----][--------B--------][------G------][-----I----]
    // 3:                         [---D--][------E-----][-------F------]    [----H----]          [--J--]
    assert_float_eq(peft_makespan, 95.0, EPSILON);

    let simple_makespan = run_scheduler(SimpleScheduler::new(), dag);
    assert_float_eq(simple_makespan, 256.0, EPSILON);
}

#[test]
fn test_chain_1() {
    let mut dag = DAG::new();
    for i in 0..10 {
        dag.add_task(&i.to_string(), (i * 10 + 20) as f64, 32, 1, 2, CoresDependency::Linear);
    }
    for i in 0..9 {
        let id = dag.add_task_output(i, &i.to_string(), (i * 100 + 200) as f64);
        dag.add_data_dependency(id, i + 1);
    }
    let input = dag.add_data_item("input", 600.);
    dag.add_data_dependency(input, 0);
    dag.add_task_output(9, "output", 700.);

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
        Vec::new(),
        NetworkConfig::constant(bandwidth, latency * 1e6),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    sim.add_resource("0", 5., 10, 1024);
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_float_eq(result, correct_result, EPSILON);
}

#[test]
fn test_chain_2() {
    let mut dag = DAG::new();
    for i in 0..10 {
        dag.add_task(&i.to_string(), (i * 10 + 20) as f64, 32, 1, 2, CoresDependency::Linear);
    }
    for i in 0..9 {
        let id = dag.add_task_output(i, &i.to_string(), (i * 100 + 200) as f64);
        dag.add_data_dependency(id, i + 1);
    }
    let input = dag.add_data_item("input", 600.);
    dag.add_data_dependency(input, 0);
    dag.add_task_output(9, "output", 700.);

    let bandwidth = 10.0;
    let latency = 0.1;

    let mut correct_result = 0.;
    for task in dag.get_tasks() {
        correct_result += task.flops as f64 / 5. / 2.;
    }
    for data_item in dag.get_data_items() {
        correct_result += (data_item.size as f64 / bandwidth + latency) * 2.;
    }

    let mut sim = DagSimulation::new(
        123,
        Vec::new(),
        NetworkConfig::constant(bandwidth, latency * 1e6),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::ViaMasterNode,
        },
    );
    sim.add_resource("0", 5., 10, 1024);
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_float_eq(result, correct_result, EPSILON);
}

#[test]
fn test_fork_join() {
    let mut dag = DAG::new();
    let root = dag.add_task("root", 10., 32, 1, 1, CoresDependency::Linear);
    let end = dag.add_task("end", 10., 32, 1, 1, CoresDependency::Linear);
    for i in 0..5 {
        let data_id = dag.add_task_output(root, &i.to_string(), 100.);
        let task_id = dag.add_task(&i.to_string(), 50., 32, 1, 1, CoresDependency::Linear);
        dag.add_data_dependency(data_id, task_id);
        let data_id = dag.add_task_output(task_id, &(i.to_string() + "_"), 200.);
        dag.add_data_dependency(data_id, end);
    }

    let bandwidth = 10.0;
    let latency = 0.1;

    let mut correct_result = (10. + 50. + 10.) / 5.;
    correct_result += 100. / bandwidth + 0.1;
    correct_result += 200. / bandwidth + 0.1;

    let mut sim = DagSimulation::new(
        123,
        Vec::new(),
        NetworkConfig::constant(bandwidth, latency * 1e6),
        Rc::new(RefCell::new(SimpleScheduler::new())),
        Config {
            data_transfer_mode: DataTransferMode::Direct,
        },
    );
    for i in 0..5 {
        sim.add_resource(&i.to_string(), 5., 1, 1024);
    }
    let runner = sim.init(dag);
    sim.step_until_no_events();
    assert!(runner.borrow().is_completed());

    let result = sim.time();
    assert_float_eq(result, correct_result, EPSILON);
}
