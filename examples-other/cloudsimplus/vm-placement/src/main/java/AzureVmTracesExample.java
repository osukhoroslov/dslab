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
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.HostResourceStats;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.util.Log;

import ch.qos.logback.classic.Level;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Simulation of Azure VM traces using CloudSim Plus.
 * The code in based on the official example:
 * https://github.com/cloudsimplus/cloudsimplus-examples/blob/master/src/main/java/org/cloudsimplus/examples/power/PowerExample.java
 *
**/
public class AzureVmTracesExample {
    // Minimum time between events (used as delay between VM creation and cloudlet scheduling!)
    private static final double MIN_TIME_BETWEEN_EVENTS = 1.;
    // Defines, between other things, the time intervals to keep Hosts CPU utilization history records
    private static final int SCHEDULING_INTERVAL = 10;
    // MIPS performance of PE
    private static final int PE_MIPS = 1000;
    // CPUs per host
    private static final int HOST_PES = 192;
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
    // Host vCPU capacity
    private static final int HOST_CPU = 192;
    // Host memory capacity in GB
    private static final int HOST_MEMORY = 320;

    public static void main(String[] args) throws Exception {
        new AzureVmTracesExample(Integer.parseInt(args[0]), args[1], args[2], Double.parseDouble(args[3]));
    }

    private AzureVmTracesExample(int host_count,
                                  String tracesVmTypesPath,
                                  String tracesVmInstancesPath,
                                  double simulationTime) throws Exception {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(Level.INFO);
        //Log.setLevel(DatacenterBroker.LOGGER, Level.ERROR);

        final CloudSim simulation = new CloudSim(MIN_TIME_BETWEEN_EVENTS);
        final var hosts = createHosts(host_count);
        final Datacenter dc = new DatacenterSimple(simulation, hosts, new VmAllocationPolicyBestFit());
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        // Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);
        broker.setVmDestructionDelay(2 * MIN_TIME_BETWEEN_EVENTS);

        var vmTypes = readVmType(tracesVmTypesPath);
        var vmInstances = readVmInstances(tracesVmInstancesPath);

        ArrayList<Vm> vmList = new ArrayList();
        ArrayList<Cloudlet> cloudletList = new ArrayList();
        for (AzureVmInstance instance: vmInstances) {
            var vmType = vmTypes.get(instance.vmTypeId);
            if (vmType == null) {
                continue;
            }

            final var vmId = instance.vmId;
            final var vmPes = Math.max((int)(vmType.cpu * HOST_CPU), 1);
            final var vmRam = Math.max((int)(vmType.memory * HOST_MEMORY), 1);
            final var vmBw = 1000;
            final var vmSize = 1000;
            var finishTime = simulationTime;
            if (instance.endTime != null && instance.endTime.getAsDouble() < simulationTime) {
                finishTime = instance.endTime.getAsDouble();
            }
            if (instance.startTime > simulationTime) {
                continue;
            }
            if (instance.startTime >= finishTime - 0.1) {
                continue;
            }

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

        final long ram = 320; //in Megabytes
        final long bw = 100000; //in Megabits/s
        final long storage = 1000000; //in Megabytes
        final var vmScheduler = new VmSchedulerTimeShared();

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

    private HashMap<String, AzureVmType> readVmType(String tracesVmTypesPath) throws Exception {
        HashMap<String, AzureVmType> records = new HashMap<String, AzureVmType>();
        try (BufferedReader br = new BufferedReader(new FileReader(tracesVmTypesPath))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                } 
                String[] values = line.split(COMMA_DELIMITER);
                AzureVmType type = new AzureVmType(values);
                records.put(type.id, type);    
            }
        }
        return records;
    }

    private ArrayList<AzureVmInstance> readVmInstances(String tracesVmInstancesPath) throws Exception {
        ArrayList<AzureVmInstance> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(tracesVmInstancesPath))) {
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

    /**
     * Creates a cloudlet with pre-defined configuration.
     *
     * @param id Cloudlet id
     * @param vm vm to run the cloudlet
     * @return the created cloudlet
     */
    private Cloudlet createCloudlet(final int id, final Vm vm, final int vmPes, final double duration) {
        final long fileSize = 1;
        final long outputSize = 1;
        final long length = (long) (duration * PE_MIPS); // in number of Million Instructions (MI)
        final int pesNumber = vmPes;
        final var utilizationModel = new UtilizationModelFull();

        return new CloudletSimple(id, length, pesNumber)
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
}
