# DSLab IaaS

## Overview

A library for cloud simulations creation.

It can be used by cloud computing academic studies to implement user specific simulations. The simulator contains a set of standard components which allow developers to focus on specific system design issues to be investigated, without concerning the low-level details of simulation. At the same time, any component can be easily extended or replaced by the user to meet their behavior requirements. As the result, the library supports simple, easy-to-understand simulations and also grants the access to low-level details.

The library enables modelling of different cloud infrastructure items such as physical machines (or [HostManager](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/host_manager.rs)), virtual machines ([VirtualMachine](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/vm.rs)) and other components of resource allocation and management. The simulation processes a chain of discrete events and their handlers brought from [dslab-core](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-core) crate. Any component implements the handler interface and use events to interact with others.

The simulation is managed via [CloudSimulation](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/simulation.rs#L25), which provides an interface to spawn new physical machines, submit virtual machines and migrate them, add new user defined components etc. As long as simulation consists of events ordered by timestamp, it is possible to run simulation for a few steps or time period. 

Basic examples of library usage can be found in [examples](https://github.com/osukhoroslov/dslab/blob/main/examples/iaas/src/main.rs) block.

## Virtual machines allocation

Virtual machines can be submitted to cloud cluster via scheduler component. The scheduler selects the most suitable physical host among all possible at the moment. The library supports multiple schedulers, thus a two-factor commit approach is used for virtual machine allocation. Conflicts are resolved by a centralized database called [PlacementStore](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/placement_store.rs#L20). After an allocation is confirmed, an allocation request is sent to the selected host.

Virtual machine can be submitted to cluster by calling `spawn_vm_now` or `spawn_vm_with_delay` function:

```rust
let sim = Simulation::new(123); // create dslab-core simulation
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

It is possible to configure the simulation whether to support resource overcommitment or not. [allow_vm_overcommit](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/config.rs#L27) parameter in simulation config enables a possibility to submit additional VMs on a physical host even if all resources on that host are allocated. Schedulers can pack virtual machines until any resource load reaches 80% threshold.

Overcommitment in simulation can be modelled by spawning virtual machines with low resource usage. In following scenario the VM actual usage is only 1vCPU and 1 GB of memory which makes it possible to add more VM on that host and utilize remaining 9 vCPUs.

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

To access host actual loads, a monitoring component is provided to any host selection algorithm. The standard library contains [BestFitThreshold](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/core/vm_placement_algorithm.rs#L87) algorithm, which selects a host with maximal CPU actual load among all possible within a given threshold. In is possible to implement new custom algorithms in this file and import them to simulation code.

## Register new components

New components can be added to `CloudSimulation` in order to implement any custom logic that cannot be performed by existing ones. We propose an example of such component by [VmMigrator](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/extensions/vm_migrator.rs#L22). It periodically checks the state of the cluster and tries to find overloaded and underloaded hosts. If there are any, it selects some VMs and migrates them to other hosts in order to turn off underloaded machines and return overloaded to normal state.

To interact with standard components, user component should implement `EventHandler` interface and get them in `patch_custom_args` method.

```rust
let migrator = cloud_sim.build_custom_component::<VmMigrator>("migrator"); // create component
migrator
    .borrow_mut()
    .patch_custom_args(5., cloud_sim.monitoring(), cloud_sim.vm_api(), cloud_sim.sim_config()); // pass required standard components
migrator.borrow_mut().init(); // initialize component, start periodic process
```

## Public traces usage

The library supports two different public traces - Huawei Cloud 2021 and Microsoft Azure 2020. The examples can be found [here](https://github.com/osukhoroslov/dslab/tree/main/examples/iaas-traces). Supporting other traces requires implementation of a dataset reader, which should support the [DatasetReader](https://github.com/osukhoroslov/dslab/blob/main/crates/dslab-iaas/src/extensions/dataset_reader.rs#L10) interface. After the reader is ready, the dataset can be submitted to simulation using this code:

```rust
let mut dataset = HuaweiDatasetReader::new(config.simulation_length); // create dataset
dataset.parse(format!("{}/Huawei-East-1.csv", dataset_path));         // parse dataset file

let scheduler_id = cloud_sim.add_scheduler("s", Box::new(FirstFit::new())); // create scheduler where to submit dataset VMs
cloud_sim.spawn_vms_from_dataset(scheduler_id, dataset);
```

That spawns plenty of events on a given scheduler that submit VMs from the dataset. Note that all parameters e.g. lifetime duration and start time should be specified during `parse_dataset` stage.
