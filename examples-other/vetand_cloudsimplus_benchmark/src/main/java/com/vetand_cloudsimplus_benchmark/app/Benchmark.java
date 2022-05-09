package com.vetand_cloudsimplus_benchmark.app;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerBestFit;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelHostSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.HostResourceStats;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmResourceStats;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.util.Log;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static java.util.Comparator.comparingLong;

/**
 * An example build on the top of Benchmark of CloudSim Plus
 * Original example
 * https://github.com/manoelcampos/cloudsimplus/blob/master/cloudsim-plus-examples/src/main/java/org/cloudsimplus/examples/power/Benchmark.java
 *
**/
public class Benchmark {
    /**
     * Defines, between other things, the time intervals
     * to keep Hosts CPU utilization history records.
     */
    private static final int SCHEDULING_INTERVAL = 10;
    private static final int HOSTS = 1000;
    private static final int HOST_PES = 144;

    /** Indicates the time (in seconds) the Host takes to start up. */
    private static final double HOST_START_UP_DELAY = 5;

    /** Indicates the time (in seconds) the Host takes to shut down. */
    private static final double HOST_SHUT_DOWN_DELAY = 3;

    /** Indicates Host power consumption (in Watts) during startup. */
    private static final double HOST_START_UP_POWER = 5;

    /** Indicates Host power consumption (in Watts) during shutdown. */
    private static final double HOST_SHUT_DOWN_POWER = 3;

    private static final int VMS = 5000;
    private static final int VM_PES = 1;

    /**
     * Defines the power a Host uses, even if it's idle (in Watts).
     */
    private static final double STATIC_POWER = 35;

    /**
     * The max power a Host uses (in Watts).
     */
    private static final int MAX_POWER = 50;

    private final CloudSim simulation;
    private DatacenterBroker broker0;
    private List<Vm> vmList;
    private List<Cloudlet> cloudletList;
    private Datacenter datacenter0;
    private final List<Host> hostList;

    public static void main(String[] args) {
        new Benchmark();
    }

    private Benchmark() {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(ch.qos.logback.classic.Level.ERROR);

        long timeStart = System.currentTimeMillis();

        simulation = new CloudSim();
        hostList = new ArrayList<>(HOSTS);
        datacenter0 = createDatacenter();
        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs and Cloudlets
        broker0 = new DatacenterBrokerBestFit(simulation);

        vmList = createVms();
        broker0.submitVmList(vmList);

        simulation.start();

        System.out.println("------------------------------- SIMULATION FOR SCHEDULING INTERVAL = " + SCHEDULING_INTERVAL+" -------------------------------");

        var timeFinish = System.currentTimeMillis();
        var timeElapsed = timeFinish - timeStart;
        System.out.println("Elapsed time is " + timeElapsed / 1000 + " seconds");
    }

    /**
     * Creates a {@link Datacenter} and its {@link Host}s.
     */
    private Datacenter createDatacenter() {
        for(int i = 0; i < HOSTS; i++) {
            final var host = createPowerHost(i);
            hostList.add(host);
        }

        final var dc = new DatacenterSimple(simulation, hostList);
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        return dc;
    }

    private Host createPowerHost(final int id) {
        final var peList = new ArrayList<Pe>(HOST_PES);
        //List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            peList.add(new PeSimple(1000));
        }

        final long ram = 4096; //in Megabytes
        final long bw = 20000; //in Megabits/s
        final long storage = 200000; //in Megabytes
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

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms() {
        final var list = new ArrayList<Vm>(VMS);

        final Random random = new Random();
        random.setSeed(47);
      
        final var vmPesDistribution = new int[] {1, 2, 4, 8, 16};
        final var vmRamDistribution = new int[] {128, 256, 512};
        final var vmBwDistribution = new int[] {250, 500, 1000, 2000};
        final var vmSizeDistribution = new int[] {2500, 5000, 10000, 20000};

        for (int i = 0; i < VMS; i++) {
            final var vmPes = vmPesDistribution[random.nextInt(5)];
            final var vmRam = vmRamDistribution[random.nextInt(3)];
            final var vmBw = vmBwDistribution[random.nextInt(4)];
            final var vmSize = vmSizeDistribution[random.nextInt(4)];

            final var vm = new VmSimple(i, 1, vmPes);
            vm.setRam(vmRam).setBw(vmBw).setSize(vmSize).enableUtilizationStats();
            list.add(vm);
        }

        return list;
    }
}
