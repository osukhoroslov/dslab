# Throughput sharing model

This model evaluates how some resource with limited throughput (e.g. network, storage or compute) is shared by several concurrent activities (e.g. data transfers or computations). Currently only fair sharing is implemented, i.e. each activity gets an equal share of resource throughput computed as `throughput / num of activities`. 

This model can be used to calculate completion times of such activities as network data transfers, storage read/write operations or compute tasks.

The dependence of resource total throughput on the number of concurrent activities, i.e. throughput degradation, can be modeled with an arbitrary used-defined function.

## Slow algorithm

This is a simple algorithm which explicitly recalculates all activities' complete times on every `insert` and `pop` call. `BinaryHeap` is used as a storage for activities, and they are sorted by their remaining volume.

Recalculation consists of 3 steps:

1) Evaluate `throughput_per_item` using degradation function
2) Evaluate `processed_volume` - amount, processed by single activity since last recalculation time
3) For each activity evaluate `remaining_volume` as previous `remaining_volume` of this activity subtracted by `processed_volume` and push updated entry to the binary heap instead of previous.
4) Update last recalculation time

## Fast algorithm
