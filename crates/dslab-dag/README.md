# DSLab DAG

## Overview

A library for simulation of DAG (directed acyclic graph) execution.
It can be used for comparing different scheduling algorithms and developing new ones.

The library provides an ability to set a set of available [resources](dslab-dag/src/resource.rs#L12), a network, a [scheduler](dslab-dag/src/scheduler.rs#L29) and a [DAG](dslab-dag/src/dag.rs#L9).
Each resource represents one [multicore Compute actor](dslab-compute/src/multicore.rs#L141) from [dslab-compute crate](dslab-compute) with given speed (in flops), memory and number of cores.
A description of network can be found in corresponding docs for [dslab-network crate](dslab-network).

Scheduler is a struct that implements trait [Scheduler](dslab-dag/src/scheduler.rs#L29) which currently has 2 callbacks -- one will be called once at the start of simulation and the other will be called on every task state change.
Each callback can return an array of [actions](dslab-dag/src/scheduler.rs#L11).
Implementations of some scheduling algorithms can be found in the folder [schedulers](dslab-dag/src/schedulers).

## DagSimulation

[DagSimulation](dslab-dag/src/dag_simulation.rs#L14) provides a wrapper around default Simulation to make it easier to run DAG simulations.

```rust
let network_model = load_network("path/to/network/file.yaml");  // for example, https://github.com/osukhoroslov/dslab/blob/main/examples/dag/networks/network1.yaml
let scheduler = rc!(refcell!(SimpleScheduler::new()));  // from https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-dag/src/schedulers/simple_scheduler.rs
let mut sim = DagSimulation::new(123, network_model, scheduler, Config { DataTransferMode::Direct });  // init DagSimulation with random seed 123
sim.load_resources("path/to/resources/file.yaml");  // for example, https://github.com/osukhoroslov/dslab/blob/main/examples/dag/resources/cluster1.yaml
let dag = DAG::from_yaml("path/to/dag/file.yaml");  // for example, https://github.com/osukhoroslov/dslab/blob/main/examples/dag/dags/diamond.yaml

let runner = sim.init(dag);  // init and start simulation
sim.step_until_no_events();  // run simulation
runner.borrow().validate_completed();  // check that all tasks in DAG are completed
```
