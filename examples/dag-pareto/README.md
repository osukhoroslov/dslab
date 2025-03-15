# DSLab DAG Pareto Example

This project demonstrates how to work with Pareto fronts in DSLab DAG:

- `dags` folder contains sample DAG generator.
- `src` folder contains the basic implementation.
- `systems` folder contains sample system configurations.

## Running Simulator

Use this command to simulate the execution of a given DAG on a given system:

```
cargo run --release -- -d DAG_PATH -s SYSTEM_PATH
```

The program will use the bi-objective MOHEFT scheduling algorithm to produce a set of Pareto-efficient schedules and will output their makespan and cost, as well as makespan and cost of the schedule obtained by the single-objective HEFT algorithm.

## Sample Systems

System configurations are described in YAML format using the following unit conventions:

- resource speed is specified in Gflop/s (floating point operations per second)
- resource memory is specified in MB
- resource price is specified in $/hour
- network bandwidth is specified in MB/s
- network latency in specified in μs
