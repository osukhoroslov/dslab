# DSLab IaaS

## Overview

A library for simulation of Infrastructure as a Service (IaaS) clouds aimed to facilitate research in cloud resource
management.

It can be used by cloud computing academic studies to implement user specific simulations. The simulator contains a set
of standard components which allow developers to focus on specific system design issues to be investigated, without
concerning the low-level details of simulation. At the same time, any component can be easily extended or replaced by
the user to meet their behavior requirements. As the result, the library supports simple, easy-to-understand simulations
and also grants the access to low-level details.

The library enables modelling of different cloud infrastructure components such as physical machines (
via [HostManager](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/host_manager.rs)), virtual
machines (via [VirtualMachine](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/vm.rs)) and
core services (VM scheduler, placement store, monitoring, etc). The simulation is implemented as a chain of discrete
events by means of [SimCore](https://github.com/systems-group/simcore). The components produce
events during the simulation and process events produced by other components via event handlers.

The simulation is configured and managed
via [CloudSimulation](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/simulation.rs). It
encapsulates all simulations components and provides convenient access to them for the user. It provides an interface to
add new physical machines, submit virtual machines and migrate them, add new user defined components etc. As long as the
simulation has some unprocessed events, it is possible to run simulation for a few steps or time period by processing
events in the order of their timestamps.

Basic examples of library usage can be
found [here](https://github.com/osukhoroslov/dslab/blob/main/examples/iaas/src/main.rs).

## Virtual machines allocation

Virtual machines can be submitted to cloud resource pool
via [Scheduler](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/scheduler.rs) component. The
scheduler processes VM allocation events and selects the physical machine (aka host) for running VM. It stores a local
copy of resource pool state, which includes information of current resource allocations on each host. Scheduler can also
access information about current hosts’ load from the monitoring component. The actual VM placement decision is
delegated to the
user-defined [VM placement algorithm](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/vm_placement_algorithm.rs).

The library supports simulating a cloud with multiple schedulers that concurrently process allocation requests. Since
each scheduler operates using its own, possibly outdated resource pool state, the schedulers’ decisions may produce
conflicts. These conflicts are resolved by a centralized database
called [PlacementStore](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/placement_store.rs) (
PS). Each scheduler sends its placement decisions to the PS. If no conflicts are detected, the allocation is committed
and an allocation request is sent to the selected host.

Virtual machine can be submitted to cluster by calling `spawn_vm_now` or `spawn_vm_with_delay` function:

```rust
let sim = Simulation::new(123); // create simcore simulation
let mut cloud_sim = CloudSimulation::new(sim, sim_config); // create cloud simulation

let host = cloud_sim.add_host("h", 30, 30); // create host with 30 vCPU-s and 30 GB of RAM 
let s = cloud_sim.add_scheduler("s", Box::new(BestFit::new())); // create scheduler which uses Best Fit packing algorithm

// spawn new VM
cloud_sim.spawn_vm_now(
10, // requires 10 vCPU-s
10, // requires 10 GB of RAM
2.0, // lifetime is 2 seconds
Box::new(ConstLoadModel::new(1.0)), // VM will use 100% of allocated CPU resources
Box::new(ConstLoadModel::new(1.0)), // VM will use 100% of allocated RAM resources
None, // possible to pass here VM ID, but in this case simulation will generate it itself
s, // submit to scheduler s
);
```

## Resource overcommitment

It is possible to configure the simulation whether to support resource overcommitment or
not. [allow_vm_overcommit](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/config.rs#L27)
parameter in simulation config enables a possibility to submit additional VMs on a physical host even if all resources
on that host are allocated. Schedulers can pack virtual machines until any resource load reaches 80% threshold.

Overcommitment in simulation can be modelled by spawning virtual machines with low resource usage. In the following
scenario the VM actual usage is only 1 vCPU and 1 GB of memory which makes it possible to add more VMs on that host and
utilize the remaining 9 vCPUs.

```rust
// spawn new VM
cloud_sim.spawn_vm_now(
10, // allocate 10 vCPU-s
10, // allocate 10 GB of RAM
2.0, // lifetime is 2 seconds
Box::new(ConstLoadModel::new(0.1)), // uses only 10% of allocated CPU
Box::new(ConstLoadModel::new(0.1)), // uses only 10% of allocated RAM
None, // possible to pass here VM ID, but in this case simulation will generate it itself
s, // submit to scheduler s
);
```

To access the actual host load, a monitoring component is provided to any VM placement algorithm. The standard library
contains [BestFitThreshold](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/vm_placement_algorithm.rs#L87)
algorithm, which selects a host with maximal actual CPU load among all feasible candidates within a given threshold. It
is possible to implement other algorithms and use them in simulations.

## Registering new components

New components can be added to `CloudSimulation` in order to implement any custom logic that cannot be performed by
existing ones. An example of such component
is [VmMigrator](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/extensions/vm_migrator.rs#L22). It
periodically checks the state of resource pool and tries to find the overloaded and underloaded hosts. If there are any,
it selects some VMs from these hosts and migrates them to other hosts in order to turn off the underloaded hosts and
return the overloaded hosts to normal state.

To interact with standard components during simulation, user component should implement `EventHandler` interface and a
method for passing references to needed components like `patch_custom_args()` in the example below.

```rust
let migrator = cloud_sim.build_custom_component::<VmMigrator>("migrator"); // create component
migrator
.borrow_mut()
.patch_custom_args(5., cloud_sim.monitoring(), cloud_sim.vm_api(), cloud_sim.sim_config()); // pass required standard components
migrator.borrow_mut().init(); // initialize component, start periodic process
```

## Public traces usage

The library supports two different public cloud traces - Huawei Cloud 2021 and Microsoft Azure 2020. The examples can be
found [here](https://github.com/osukhoroslov/dslab/tree/main/examples/iaas-traces). Supporting other traces requires the
implementation of a dataset reader, which should support
the [DatasetReader](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/extensions/dataset_reader.rs#L10)
trait. After the reader is ready, the dataset can be used in simulation as follows:

```rust
let mut dataset = HuaweiDatasetReader::new(config.simulation_length); // create dataset
dataset.parse(format!("{}/Huawei-East-1.csv", dataset_path));         // parse dataset file

let scheduler_id = cloud_sim.add_scheduler("s", Box::new(FirstFit::new())); // create scheduler where to submit dataset VMs
cloud_sim.spawn_vms_from_dataset(scheduler_id, dataset);
```

The last call injects the VM allocation and release events from the trace into the simulation by passing them to a given
scheduler. Note that all parameters like VM start time and lifetime duration should be set during the dataset parsing.
