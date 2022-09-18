import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyBestFit;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerBestFit;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.HostResourceStats;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.util.Log;

import ch.qos.logback.classic.Level;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Simulation of Azure VM traces using CloudSim Plus.
 * The code in based on the official example:
 * https://github.com/cloudsimplus/cloudsimplus-examples/blob/master/src/main/java/org/cloudsimplus/examples/power/PowerExample.java
 *
**/
public class AzureVmTraceExample {
    // Defines, between other things, the time intervals to keep Hosts CPU utilization history records
    private static final int SCHEDULING_INTERVAL = 60;
    // MIPS performance of PE
    private static final int PE_MIPS = 1000;
    // CPUs per host
    private static final int HOST_PES = 64;
    // Host memory capacity in GB
    private static final int HOST_MEMORY = 128;
    // Indicates the time (in seconds) the Host takes to start up
    private static final double HOST_START_UP_DELAY = 0;
    // Indicates the time (in seconds) the Host takes to shut down
    private static final double HOST_SHUT_DOWN_DELAY = 3;
    // Indicates Host power consumption (in Watts) during startup
    private static final double HOST_START_UP_POWER = 5;
    // Indicates Host power consumption (in Watts) during shutdown
    private static final double HOST_SHUT_DOWN_POWER = 3;
    // Defines the power a Host uses, even if it's idle (in Watts)
    private static final double STATIC_POWER = 35;
    // The max power a Host uses (in Watts)
    private static final int MAX_POWER = 50;
    // Just comma (,) symbol
    private static final String COMMA_DELIMITER = ",";

    public static void main(String[] args) throws Exception {
        new AzureVmTraceExample(args[0], args[1], Double.parseDouble(args[2]), Integer.parseInt(args[3]));
    }

    private AzureVmTraceExample(String vmTypesPath, String vmInstancesPath,
                                double simulationTime, int host_count) throws Exception {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(Level.ERROR);
        //Log.setLevel(DatacenterBroker.LOGGER, Level.ERROR);

        final CloudSim simulation = new CloudSim();
        final var hosts = createHosts(host_count);
        final Datacenter dc = new DatacenterSimple(simulation, hosts, new VmAllocationPolicyBestFit());
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        // Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);
        broker.setVmDestructionDelay(1);

        var vmTypes = readVmTypes(vmTypesPath);
        var vmInstances = readVmInstances(vmInstancesPath);

        List<Vm> vmList = new ArrayList<>();
        List<Cloudlet> cloudletList = new ArrayList<>();
        for (AzureVmInstance instance: vmInstances) {
            if (instance.startTime < 0) {
                continue;
            }
            if (instance.startTime > simulationTime) {
                break;
            }
            var finishTime = simulationTime;
            if (instance.endTime.isPresent() && instance.endTime.getAsDouble() < simulationTime) {
                finishTime = instance.endTime.getAsDouble();
            }

            final var vmId = instance.vmId;
            var vmType = vmTypes.get(instance.vmTypeId);
            final var vmPes = Math.max((int)(vmType.cpu * HOST_PES), 1);
            final var vmRam = Math.max((int)(vmType.memory * HOST_MEMORY), 1);
            final var vmBw = 1000;
            final var vmSize = 1000;
            final var vm = new VmSimple(instance.vmId, PE_MIPS, vmPes);
            final var duration = finishTime - instance.startTime;
            vm.setRam(vmRam).setBw(vmBw).setSize(vmSize).enableUtilizationStats();
            vm.setSubmissionDelay(instance.startTime);
            vm.setStopTime(finishTime);
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vm.enableUtilizationStats();
            vmList.add(vm);

            final var cloudlet = createCloudlet(vmId, vm, vmPes, duration);
            cloudlet.setExecStartTime(instance.startTime);
            cloudletList.add(cloudlet);
        }
        System.out.println("Number of VMs is " + vmList.size());

        long timeStart = System.currentTimeMillis();

        broker.submitVmList(vmList);
        broker.submitCloudletList(cloudletList);

        simulation.start();

        var timeFinish = System.currentTimeMillis();
        var timeElapsed = timeFinish - timeStart;
        System.out.println("Elapsed time is " + timeElapsed / 1000.0 + " seconds");
        printHostCpuUtilizationAndPowerConsumption(hosts);
    }

    // CloudSim --------------------------------------------------------------------------------------------------------

    private List<Host> createHosts(int host_count) {
        final List<Host> hostList = new ArrayList<>(host_count);
        for(int i = 0; i < host_count; i++) {
            final var host = createPowerHost(i);
            hostList.add(host);
        }
        return hostList;
    }

    private Host createPowerHost(final int id) {
        final var peList = new ArrayList<Pe>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(PE_MIPS));
        }

        final long ram = HOST_MEMORY * 1024; //in Megabytes
        final long bw = 100000; //in Megabits/s
        final long storage = 1000000; //in Megabytes
        final var vmScheduler = new VmSchedulerSpaceShared();

        final var host = new HostSimple(ram, bw, storage, peList);

        final var powerModel = new PowerModelHostSimple(MAX_POWER, STATIC_POWER);
        powerModel.setStartupDelay(HOST_START_UP_DELAY)
                  .setShutDownDelay(HOST_SHUT_DOWN_DELAY)
                  .setStartupPower(HOST_START_UP_POWER)
                  .setShutDownPower(HOST_SHUT_DOWN_POWER);

        host.setVmScheduler(vmScheduler).setPowerModel(powerModel);
        host.setId(id);
        host.enableUtilizationStats();

        return host;
    }

    private Cloudlet createCloudlet(final int id, final Vm vm, final int vmPes, final double duration) {
        final long fileSize = 1;
        final long outputSize = 1;
        final long length = (long) (duration * PE_MIPS); // in number of Million Instructions (MI)
        final var utilizationModel = new UtilizationModelFull();

        return new CloudletSimple(id, length, vmPes)
            .setFileSize(fileSize)
            .setOutputSize(outputSize)
            .setUtilizationModel(utilizationModel)
            .setVm(vm);
    }

    private void printHostCpuUtilizationAndPowerConsumption(final List<Host> hosts) {
        double accumulatedCPUUtilization = 0;
        for (Host host: hosts) {
            final HostResourceStats cpuStats = host.getCpuUtilizationStats();

            // The total Host's CPU utilization for the time specified by the map key
            final double utilizationPercentMean = cpuStats.getMean();
            accumulatedCPUUtilization += utilizationPercentMean * 100;
        }
        System.out.printf("Mean host CPU utilization is %.1f%%", 
            accumulatedCPUUtilization / hosts.size());
        System.out.println();
    }

    // Dataset ---------------------------------------------------------------------------------------------------------

    private static class AzureVmType {
        String id;
        String vmTypeId;
        double cpu;
        double memory;

        public AzureVmType(String[] values) {
            this.id = values[0];
            this.vmTypeId = values[1];
            this.cpu = Double.parseDouble(values[2]);
            this.memory = Double.parseDouble(values[3]);
        }
    }

    private static class AzureVmInstance {
        int vmId;
        String vmTypeId;
        double startTime;
        OptionalDouble endTime;

        public AzureVmInstance(String[] values) {
            this.vmId = Integer.parseInt(values[0]);
            this.vmTypeId = values[1];
            this.startTime = Double.parseDouble(values[2]) * 86400;
            if (!values[3].equals("none") && !values[3].equals("")) {
                this.endTime = OptionalDouble.of(Double.parseDouble(values[3]) * 86400) ;
            } else {
                this.endTime = OptionalDouble.empty();
            }
        }
    }

    private HashMap<String, AzureVmType> readVmTypes(String vmTypesPath) throws Exception {
        HashMap<String, AzureVmType> records = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(vmTypesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                String[] values = line.split(COMMA_DELIMITER);
                AzureVmType type = new AzureVmType(values);
                records.put(type.vmTypeId, type);
            }
        }
        return records;
    }

    private ArrayList<AzureVmInstance> readVmInstances(String vmInstancesPath) throws Exception {
        ArrayList<AzureVmInstance> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(vmInstancesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                }
                line += ", none";
                String[] values = line.split(COMMA_DELIMITER);
                AzureVmInstance instance = new AzureVmInstance(values);
                records.add(instance);
            }
        }
        return records;
    }
}
