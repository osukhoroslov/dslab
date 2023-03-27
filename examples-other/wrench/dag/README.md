# Simple workflow example on WRENCH

Based on [official WRENCH example](https://github.com/wrench-project/wrench/tree/master/examples/workflow_api/real-workflow-example).

## Build

Install SimGrid and WRENCH.

```
cmake . && make
```

## Run Examples

```
./wrench-example-real-workflow PLATFORM_PATH WORKFLOW_PATH
```

To measure memory and time usage:

```
command time -f '%Mkb\n%es' ./wrench-example-real-workflow PLATFORM_PATH WORKFLOW_PATH
```

Workflows for tests can be generated using [this script](../../../examples/dag-benchmark/dags/generator.py).

Use `--wrench-mailbox-pool-size=1000000` to increase the maximum allowed number of mailboxes to prevent WRENCH from failing on large workflows.
