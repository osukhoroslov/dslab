# DSLab Compute

This library includes models of computing resource which can perform computations represented as compute tasks. The resource is characterized by the number of CPU cores, their speed in flop/s and amount of memory. The compute task is characterized by the amount of computations in flops, number of used cores, and amount of used memory. 

Two models are currently implemented:

- `singlecore` model implements resource with a single "core" supporting concurrent execution of arbitrary number of tasks. The core speed is evenly shared between the currently running tasks. The task completion time is determined by the amount of computations and the core share. Each time a task is completed or a new task is submitted, the core shares and completion times of all running tasks are updated accordingly.
- `multicore` model implements resource with multiple cores which supports execution of parallel tasks. In this model, the compute task can specify the minimum and maximum number of used cores, and provide a function which defines the dependence of parallel speedup on the number of used cores. Each core can only be used by one task. The cores allocation for each task is computed upon the task arrival and, in contrast to previous model, is not changed during the task execution. This model also supports the manual allocation and release of cores and memory.

Documentation is available [here](https://osukhoroslov.github.io/dslab/docs/dslab_compute/index.html).

## Examples

- [compute-singlecore](https://github.com/osukhoroslov/dslab/tree/main/examples/compute-singlecore): demonstrates the use of `singlecore` model.
- [compute-multicore](https://github.com/osukhoroslov/dslab/tree/main/examples/compute-multicore): demonstrates the use of `multicore` model.