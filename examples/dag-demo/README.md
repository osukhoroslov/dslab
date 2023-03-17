# DSLab DAG Demo

This project demonstrates the use of DSLab DAG for simulation of DAG execution:

- `dags` folder contains sample DAGs
- `src` folder contains the basic simulator implementation you can reuse in your projects
- `systems` folder contains sample system configurations

## Running Simulator

Use this command to simulate the execution of a given DAG on a given system:

```
cargo run --release -- -d DAG_PATH -s SYSTEM_PATH
```

The program will simulate the DAG execution using all supported scheduling algorithms and will output the obtained 
DAG execution times (makespans).

Use this command to see additional options:

```
cargo run --release -- -h
```

If you enable saving of trace logs you can later visualize them using the [dag-draw](../../tools/dag-draw) tool.

## Sample DAGs

1. Small examples in experimental format supported by DSLab DAG (YAML format)
   - `diamond.yaml`
   - `map-reduce.yaml`
2. Random DAGs generated with [DAGGEN](https://github.com/frs69wq/daggen) (DOT format)
   - `daggen-*.dot` files
3. Synthetic DAGs based on scientific workflows generated with [Workflow Generator](https://github.com/pegasus-isi/WorkflowGenerator) (DAX format)
   - `cybershake-100.xml`
   - `inspiral-100.xml`
4. Real DAGs describing executions of scientific workflows from [WfCommons](https://wfcommons.org/instances) project (JSON format)
   - `1000genome-chameleon-16ch-250k-001.json`
   - `epigenomics-chameleon-ilmn-2seq-50k-001.json`
   - `montage-chameleon-dss-125d-001.json`

### YAML format

The experimental YAML format uses the following unit conventions:

- task size (flops) is specified in Gflops
- task memory demand is specified in MB 
- input/output data sizes are specified in MB 

## Sample Systems

System configurations are described in YAML format using the following unit conventions:

- resource speed is specified in GFLOPS
- resource memory is specified in MB
- network bandwidth is specified in MB/s
- network latency in specified in μs
