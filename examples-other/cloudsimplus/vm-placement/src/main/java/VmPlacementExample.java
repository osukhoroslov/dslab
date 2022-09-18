import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyFirstFit;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerBestFit;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.util.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Simulation of VM placement using CloudSim Plus.
 * The code in based on the official example:
 * https://github.com/cloudsimplus/cloudsimplus-examples/blob/master/src/main/java/org/cloudsimplus/examples/power/PowerExample.java
 *
**/
public class VmPlacementExample {
    // Defines, between other things, the time intervals to keep Hosts CPU utilization history records
    private static final int SCHEDULING_INTERVAL = 10;
    // MIPS performance of PE
    private static final int PE_MIPS = 1000;
    // CPUs per host
    private static final int HOST_PES = 144;
    // Host memory capacity in Megabytes
    private static final int HOST_MEMORY = 204800;
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

    public static void main(String[] args) {
        int host_count = Integer.parseInt(args[0]);
        int vm_count = Integer.parseInt(args[1]);
        new VmPlacementExample(host_count, vm_count);
    }

    private VmPlacementExample(int host_count, int vm_count) {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(ch.qos.logback.classic.Level.ERROR);

        long timeStart = System.currentTimeMillis();

        final CloudSim simulation = new CloudSim();
        final Datacenter dc = new DatacenterSimple(simulation, createHosts(host_count), new VmAllocationPolicyFirstFit());
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);

        List<Vm> vmList = createVms(vm_count);
        broker.submitVmList(vmList);

        simulation.start();

        var timeFinish = System.currentTimeMillis();
        var timeElapsed = timeFinish - timeStart;
        System.out.println("Elapsed time is " + timeElapsed / 1000.0 + " seconds");
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

        final long ram = HOST_MEMORY; //in Megabytes
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

    private List<Vm> createVms(int vm_count) {
        final var list = new ArrayList<Vm>(vm_count);

        final Random random = new Random(47);
        final var vmPesDistribution = new int[] {1, 2, 4, 8};
        final var vmRamDistribution = new int[] {512, 1024, 2048};
        final var vmBwDistribution = new int[] {250, 500, 1000, 2000};
        final var vmSizeDistribution = new int[] {2500, 5000, 10000, 20000};

        for (int i = 0; i < vm_count; i++) {
            final var vmPes = vmPesDistribution[random.nextInt(4)];
            final var vmRam = vmRamDistribution[random.nextInt(3)];
            final var vmBw = vmBwDistribution[random.nextInt(4)];
            final var vmSize = vmSizeDistribution[random.nextInt(4)];

            final var vm = new VmSimple(i, PE_MIPS, vmPes);
            vm.setRam(vmRam).setBw(vmBw).setSize(vmSize).enableUtilizationStats();
            vm.setCloudletScheduler(new CloudletSchedulerSpaceShared());
            list.add(vm);
        }

        return list;
    }
}
