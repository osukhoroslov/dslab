# DSLab Common Models

This crate contains implementations of some versatile models used by other DSLab libraries.

## Throughput sharing model

This model evaluates how some resource with limited throughput (e.g. network, storage or compute) is shared by several concurrent activities (e.g. data transfers or computations). It implements fair sharing, i.e. each activity gets an equal share of resource throughput computed as `throughput / num of activities`.

This model can be used to calculate completion times of such activities as network data transfers, storage read/write operations or compute tasks.

### Slow algorithm

TODO

### Fast algorithm

TODO
