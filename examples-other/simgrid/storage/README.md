# Storage example & benchmark

This is a SimGrid example corresponding to [storage-disk-benchmark](../../../examples/storage-disk-benchmark). It starts given amount of disk read requests, randomly distributed on multiple disks. Request size and start time are randomized too.

## Build

Run `make storage` from build directory.

## Arguments
 
| name               | meaning                     | default value               |
|--------------------|-----------------------------|-----------------------------|
| `--requests`       | Number of requests (>= 1)   | `1`                         |
| `--disks`          | Number of disks (>= 1)      | `1`                         |
| `--max-size`       | Maximal size (>= 1)         | `1000000006`                |
| `--max-start-time` | Maximal activity start time | `0`, so all will start at `0` |

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

## Comparing script

There is a [script](./compare-with-dslab.py) for comparing results evaluated by SimGrid and DSLab. Argument format is the same as for examples.