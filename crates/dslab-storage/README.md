# DSLab storage crate

This crate models such storage components as disk and file system.

## Disk

Simple disk model has two main methods - `read` and `write`, and some utility functions as `mark_free` or `get_used_space`. It can be created by `new_simple` function if bandwidths are fixed.

There is also a support for __bandwidth models__ - methods that provide bandwidth for given size. Constant, randomized and empirical models are preset on this crate and arbitrary used-defined models can be defined by user.

This model of disk **does not** support throughput sharing, so disk can process only one request on each time.

## Shared disk

This is an alternative disk model, focusing on throughput sharing model. It depends on [dslab-models](../dslab-models/) crate and transfers all computations to abstract fair sharing model. Methods set is the same as for simple disk model.

## File system

File system model is built on the top of disk model. It provides common methods for manipulating with it such as creation and deletion of files, mounting and unmounting disks, reading and writing files.

This model supports using several disks, mounted on distinct mount points, just as there is in real file system.
