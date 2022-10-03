# VM Placement Examples in CloudSim Plus

Examples simulating the placement of VMs in a cloud data center.

## Building

1) Install Java JDK 17
2) Install Maven
3) Run `mvn package`

## Examples

### VmPlacementExample

This example is analogous to [iaas-benchmark](https://github.com/osukhoroslov/dslab/tree/main/examples/iaas-benchmark). The specified number of virtual machines with varying characteristics are created in the beginning of the simulation and passed to the scheduler. The scheduler uses FirstFit algorithm to assign the virtual machines to the hosts. The simulation proceeds until all virtual machines are scheduled and started on the hosts.

Running:

```
java -cp target/vm-placement-1.0.jar VmPlacementExample HOST_COUNT VM_COUNT
```

### AzureVmTraceExample

This example is analogous to [iaas-traces](https://github.com/osukhoroslov/dslab/tree/main/examples/iaas-traces) for Azure trace. The VMs are instantiated from the trace with specified simulation length limit and then executed on resource pool with specified configuration using a scheduler with BestFit algorithm.

Running:

```
java -cp target/vm-placement-1.0.jar AzureVmTraceExample VM_TYPES_PATH VM_INSTANCES_PATH SIMULATION_LENGTH HOST_COUNT
```

### HuaweiVmTraceExample

This example is analogous to [iaas-traces](https://github.com/osukhoroslov/dslab/tree/main/examples/iaas-traces) for Huawei Cloud trace. The VMs are instantiated from the trace with specified simulation length limit and then executed on resource pool with specified configuration using a scheduler with BestFit algorithm.

Running:

```
java -cp target/vm-placement-1.0.jar HuaweiVmTraceExample TRACE_PATH SIMULATION_LENGTH HOST_COUNT
```
