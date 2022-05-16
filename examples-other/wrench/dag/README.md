# Simple workflow example on Wrench

## Build

Install SimGrid and Wrench.

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
