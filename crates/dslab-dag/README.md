# DSLab DAG

A library for studying the scheduling of computations represented as directed acyclic graphs (DAG), such as scientific workflows or data-parallel jobs, in distributed systems. It allows to describe a computational DAG and simulate its execution in a given distributed system using the specified scheduling algorithm (including the user-defined one).

The distributed system is modeled as a set of computing [resources](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/resource.rs) connected with network. Each resource is described by the number of CPU cores, their speed in flop/s and amount of memory. Resources can execute compute [tasks](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/task.rs) described by the amount of computations in flops, the minimum and maximum number of used cores, and the amount of used memory. Each task also has a function which defines the dependence of parallel speedup on the number of used cores. Currently, the allocation of cores for each task is computed upon the task arrival and is not changed during the task execution. The resource implementation is based on the [multicore](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute/src/multicore.rs) compute model from the [dslab-compute](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute) crate. The network model is provided by the [dslab-network](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-network) crate.

The computational [DAG](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-dag/src/dag.rs) is modeled as a set of tasks with data dependencies. Each [task](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/task.rs) can produce one or more data items (task outputs) and consume (as task inputs) data items produced by other tasks. Entry tasks consume separate data items corresponding to the DAG inputs. The data dependencies between the tasks define constraints on task execution - a task cannot start its execution on some resource until all its inputs are produced (parent tasks are completed) and transferred to this resource.

The scheduling of DAG tasks, i.e. assigning tasks to resources and defining the order of task execution on each resource, is performed by the implementation of [Scheduler](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-dag/src/scheduler.rs) trait. This trait includes two callback methods which are called at the start of DAG execution and on every task state change respectively. Each method can return one or multiple actions corresponding to decisions made by the scheduler (assign task to resource, transfer data item between resources, etc). This approach allows to implement and test arbitrary static or dynamic scheduling algorithms. The library includes several ready-to-use implementations in the [schedulers](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/schedulers) folder.

[DagSimulation](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-dag/src/dag_simulation.rs) provides a convenient API for configuring and running simulations. Here is a small example:

```rust
use std::rc::Rc;
use std::cell::RefCell;

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::data_item::DataTransferMode;
use dslab_dag::network::load_network;
use dslab_dag::runner::Config;
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

// load network model configuration
let network_model = load_network("../../examples/dag/networks/network1.yaml");
// use simple scheduler implementation
let scheduler = Rc::new(RefCell::new(SimpleScheduler::new()));
// create simulation with random seed 123
let mut sim = DagSimulation::new(123, network_model, scheduler, Config { data_transfer_mode: DataTransferMode::Direct });
// load resources configuration
sim.load_resources("../../examples/dag/resources/cluster1.yaml");
// read DAG from YAML file
let dag = DAG::from_yaml("../../examples/dag/dags/diamond.yaml");

// init simulation
let runner = sim.init(dag);
// run simulation until completion
sim.step_until_no_events();
// check that all tasks in DAG are completed
runner.borrow().validate_completed();
```

More examples can be found [here](https://github.com/osukhoroslov/dslab/tree/main/examples) in folders with "dag" prefix. 
