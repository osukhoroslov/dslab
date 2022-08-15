# Throughput sharing model

This model evaluates how some resource with limited throughput (e.g. network, storage or compute) is shared by several concurrent activities (e.g. data transfers or computations). Currently only fair sharing is implemented, i.e. each activity gets an equal share of resource throughput computed as `throughput / num of activities`. 

This model can be used to calculate completion times of such activities as network data transfers, storage read/write operations or compute tasks.

The dependence of resource total throughput on the number of concurrent activities, i.e. throughput degradation, can be modeled with an arbitrary used-defined function.

## Slow algorithm

TODO

## Fast algorithm

TODO
