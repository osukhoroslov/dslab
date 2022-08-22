# DSLab DAG

## Overview

A library for simulation of DAG (directed acyclic graph) execution.
It can be used for comparing different scheduling algorithms and developing new ones.

The library provides an ability to set a set of available [resources](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/resource.rs#L12), a network, a [scheduler](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/scheduler.rs#L29) and a [DAG](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/dag.rs#L9).
Each resource represents one [multicore Compute actor](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute/src/multicore.rs#L141) from [dslab-compute crate](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute) with given speed (in flops), memory and number of cores.
A description of network can be found in corresponding docs for [dslab-network crate](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-network).

Scheduler is a struct that implements trait [Scheduler](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/scheduler.rs#L29) which currently has 2 callbacks -- one will be called once at the start of simulation and the other will be called on every task state change.
Each callback can return an array of [actions](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/scheduler.rs#L11).
Implementations of some scheduling algorithms can be found in the folder [schedulers](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/schedulers).

## DagSimulation

[DagSimulation](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-dag/src/dag_simulation.rs#L14) provides a wrapper around default Simulation to make it easier to run DAG simulations.

```rust
use std::rc::Rc;
use std::cell::RefCell;

use dslab_dag::dag::DAG;
use dslab_dag::dag_simulation::DagSimulation;
use dslab_dag::network::load_network;
use dslab_dag::runner::{Config, DataTransferMode};
use dslab_dag::schedulers::simple_scheduler::SimpleScheduler;

let network_model = load_network("../../examples/dag/networks/network1.yaml");  // https://github.com/osukhoroslov/dslab/blob/main/examples/dag/networks/network1.yaml
let scheduler = Rc::new(RefCell::new(SimpleScheduler::new()));  // from https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-dag/src/schedulers/simple_scheduler.rs
let mut sim = DagSimulation::new(123, network_model, scheduler, Config { data_transfer_mode: DataTransferMode::Direct });  // init DagSimulation with random seed 123
sim.load_resources("../../examples/dag/resources/cluster1.yaml");  // https://github.com/osukhoroslov/dslab/blob/main/examples/dag/resources/cluster1.yaml
let dag = DAG::from_yaml("../../examples/dag/dags/diamond.yaml");  // https://github.com/osukhoroslov/dslab/blob/main/examples/dag/dags/diamond.yaml

let runner = sim.init(dag);  // init and start simulation
sim.step_until_no_events();  // run simulation
runner.borrow().validate_completed();  // check that all tasks in DAG are completed
```
