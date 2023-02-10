use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_compute::multicore::CoresDependency;
use dslab_dag::dag::DAG;

/// Example includes DAG and system configuration.
pub struct Example {
    pub dag: DAG,
    pub resources: &'static str,
    pub network: &'static str,
}

/// Creates example by its name.
pub fn create_example(example_name: &str) -> Example {
    match example_name {
        "Diamond" => diamond(),
        "MapReduce" => map_reduce(),
        "Montage" => montage(),
        "Epigenomics" => epigenomics(),
        "ReuseFiles" => reuse_files(),
        _ => panic!("Unknown example: {}", example_name),
    }
}

/// Simple diamond-shaped DAG with four tasks.
pub fn diamond() -> Example {
    Example {
        dag: DAG::from_yaml("dags/diamond.yaml"),
        resources: "resources/cluster3.yaml",
        network: "networks/network4.yaml",
    }
}

/// MapReduce computation with two map tasks and four reduce tasks.
pub fn map_reduce() -> Example {
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

    Example {
        dag,
        resources: "resources/cluster1.yaml",
        network: "networks/network1.yaml",
    }
}

/// Montage scientific workflow.
pub fn montage() -> Example {
    Example {
        dag: DAG::from_dot("dags/Montage.dot"),
        resources: "resources/cluster2.yaml",
        network: "networks/network3.yaml",
    }
}

/// Epigenomics scientific worklow.
pub fn epigenomics() -> Example {
    Example {
        dag: DAG::from_dax("dags/Epigenomics_100.xml", 1000.),
        resources: "resources/cluster2.yaml",
        network: "networks/network2.yaml",
    }
}

/// Randomly generated DAG consisting of two layers of tasks
/// with some outputs of layer 1 consumed by multiple tasks of layer 2.  
pub fn reuse_files() -> Example {
    let mut dag = DAG::new();

    let input = dag.add_data_item("input", 128);

    let mut rng = Pcg64::seed_from_u64(456);

    let a_cnt = 10;
    let b_cnt = 10;
    let deps_cnt = 3;

    for i in 0..a_cnt {
        let task = dag.add_task(&format!("a{}", i), 100, 128, 1, 2, CoresDependency::Linear);
        dag.add_data_dependency(input, task);
        dag.add_task_output(task, &format!("a{}_out", i), 10);
    }

    for i in 0..b_cnt {
        let task = dag.add_task(&format!("b{}", i), 100, 128, 1, 2, CoresDependency::Linear);
        let mut deps = (0..deps_cnt).map(|_| rng.gen_range(0..a_cnt) + 1).collect::<Vec<_>>();
        deps.sort();
        deps.dedup();
        for dep in deps.into_iter() {
            dag.add_data_dependency(dep, task);
        }
        dag.add_task_output(task, &format!("b{}_out", i), 10);
    }

    Example {
        dag,
        resources: "resources/cluster1.yaml",
        network: "networks/network1.yaml",
    }
}
