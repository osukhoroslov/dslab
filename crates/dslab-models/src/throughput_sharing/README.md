# Throughput sharing model

This model evaluates how some resource with limited throughput (e.g. network, storage or compute) is shared by several concurrent activities (e.g. data transfers or computations). Currently, only fair sharing is implemented, i.e. each activity gets an equal share of resource throughput computed as `throughput / num of activities`. 

This model can be used to calculate completion times of such activities as network data transfers, storage read/write operations or compute tasks.

The dependence of resource total throughput on the number of concurrent activities, i.e. throughput degradation, can be modeled with a used-defined _resource throughput function_.

The dependence of effective activity throughput on its properties or the variability of throughput can be modeled with a used-defined _activity factor function_.

## Slow algorithm

This is a simple algorithm which explicitly recalculates all activities' completion times on every `insert` and `pop` call. `BinaryHeap` is used as a storage for activities, and they are sorted by their remaining volume.

Recalculation consists of 3 steps:

1) Update `throughput_per_item` using the resource throughput function.
2) Compute `processed_volume` - amount of work done by each activity since the last recalculation time.
3) For each activity update its `remaining_volume` by subtracting the `processed_volume` and push the updated entry to the new binary heap.
4) Update the last recalculation time.

## Fast algorithm

Algorithm can be optimized based on the fairness guarantee. If activities stored in the heap are ordered by their remaining volume, then reorderings in the heap are not possible because each activity gets equal throughput ratio at each time interval. That is why a full scan of the heap on each step is ineffective and can be avoided by using a simple metric described below.

Total work per activity or simply _total work (TW)_ at time moment _t_ is calculated as a volume processed by the resource _per activity_ since the resource started till _t_. When there is a single activity using the resource, the total work is being increased by the full resource throughput for every time unit. When there are _N_ concurrent activities, the total work is being increased by the full throughput divided by _N_.

For every activity in the system the following equation is satisfied: _TW(start_time) + volume = TW(end_time)_. So, when a new activity is placed into the model, it calculates _TW(end_time)_ as current _TW_ + activity volume. The computed value, called activity _finish work_, is inserted in the heap. Activities are popped from the heap in the ascending order of their finish work.

The total work is updated efficiently too: instead of adding some volume every time unit, it is incremented only when some activity is inserted or popped from the model. The increment value is calculated as multiplication of time passed since the last update and throughput per activity during this period.

Note that the total work is always increasing. To avoid the overflow, each time the total work is above _1E+12_ it is reset to 0 and the finish work of each activity is reduced by the old value of total work. This is correct because the activities' order does not change, and the finish times are preserved too, since they are calculated based on the difference between finish work and total work, which also does not change.
