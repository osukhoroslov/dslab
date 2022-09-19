# Throughput sharing model

This model evaluates how some resource with limited throughput (e.g. network, storage or compute) is shared by several concurrent activities (e.g. data transfers or computations). Currently, only fair sharing is implemented, i.e. each activity gets an equal share of resource throughput computed as `throughput / num of activities`. 

This model can be used to calculate completion times of such activities as network data transfers, storage read/write operations or compute tasks.

The dependence of resource total throughput on the number of concurrent activities, i.e. throughput degradation, can be modeled with an arbitrary used-defined function.

## Slow algorithm

This is a simple algorithm which explicitly recalculates all activities' completion times on every `insert` and `pop` call. `BinaryHeap` is used as a storage for activities, and they are sorted by their remaining volume.

Recalculation consists of 3 steps:

1) Update `throughput_per_item` using the degradation function.
2) Compute `processed_volume` - amount of work done by each activity since the last recalculation time.
3) For each activity update its `remaining_volume` by subtracting the `processed_volume` and push the updated entry to the new binary heap.
4) Update the last recalculation time.

## Fast algorithm

Algorithm can be optimized based on the fairness guarantee. If activities stored in the heap are ordered by their remaining volume, then reorderings in the heap are not possible because each activity gets equal throughput ratio at each time interval.

That is why full scan of the heap on each step is ineffective and can be replaced by storing some metadata next to the heap, equal for all activities.

*Total work* (`TW`) will be used for this metadata. *Total work* of time moment `t` is calculated as volume, processed by a single activity, since the system started till `t`. So, when there is single activity in the system, total work is increased by full bandwidth for every time unit, and when there are `N` activities, total work is being increased by full bandwidth divided by `N`.

It is clear that for every activity in the system the equation `TW(start_time) + volume = TW(end_time)` is satisfied.

So, when new activity is placed into model, it calculates `TW(end_time)` as current TW + activity volume. This, called `finish_work`, is inserted in the heap. Activities are popped from the heap in ascending order of `finish_work`.

Total work is calculated effectively, too: instead of adding volume every time unit, total work is updated only when activity is inserted or popped from the model. Delta is calculated as multiplication of time delta since last update and throughput per activity on this period.

It can be noticed that total work is always increasing. This can lead to overflow, so there is a periodic truncation procedure, which resets total work to 0 and subtracts all activities' finish works by old total work value. This is correct because activities order does not change and finish times are preserved too - they are calculated based on `finish_work` and `total_work` difference, which does not change.
