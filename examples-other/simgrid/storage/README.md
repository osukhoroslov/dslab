# Storage example & benchmark

This is a SimGrid example corresponding to [storage-shared-disk-benchmark](../../../examples/storage-shared-disk-benchmark). It executes the specified number of disk read requests with random size and start time. The requests are randomly distributed across the specified number of disks.

## Build

Run `make storage` from build directory.

## Arguments
 
| name               | meaning                     | default value               |
|--------------------|-----------------------------|-----------------------------|
| `--requests`       | Number of requests (>= 1)   | `1`                         |
| `--disks`          | Number of disks (>= 1)      | `1`                         |
| `--max-size`       | Maximal size (>= 1)         | `1000000006`                |
| `--max-start-time` | Maximal request start time | `0`, so all will start at `0` |

## Run

Example:

```
./bin/storage \
    --log=root.thres:info \
    --requests 10 \
    --disks 10 \
    --max-size 100 \
    --max-start-time 100
```

## Comparing results with DSLab

There is a [script](./compare-with-dslab.py) for running both SimGrid and DSLab implementations and comparing their results. Argument format is the same as for examples. You should set environment variable `DSLAB_BASE_DIR` before using and build both DSLab and SimGrid examples.