# Simple workflow example on WRENCH

## Build

Install SimGrid and WRENCH.

```
cmake . && make
```

## Run Examples

```
./wrench-example-real-workflow cloud_batch_platform.xml ../../../examples/dag-benchmark/dags/montage.json
```

To measure memory and time usage:

```
command time -f '%Mkb\n%es' ./wrench-example-real-workflow cloud_batch_platform.xml ../../../examples/dag-benchmark/dags/montage.json
```

More workflows can be generated using https://docs.wfcommons.org/en/latest/generating_workflows.html.

Use `--wrench-mailbox-pool-size=1000000` to increase maximum allowed number of mailboxes to prevent Wrench from failing on large graphs.
