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
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.util.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Simulation of Huawei VN traces using CloudSim Plus.
 * The code in based on the official example:
 * https://github.com/cloudsimplus/cloudsimplus-examples/blob/master/src/main/java/org/cloudsimplus/examples/power/PowerExample.java
 *
**/
public class VmTracesExample {
    // Defines traces file path
    private static final String TRACES_PATH = "../../Huawei-East-1.csv";
    // Defines, between other things, the time intervals to keep Hosts CPU utilization history records
    private static final int SCHEDULING_INTERVAL = 10;
    // CPUs per host
    private static final int HOST_PES = 192;
    // Indicates the time (in seconds) the Host takes to start up
    private static final double HOST_START_UP_DELAY = 5;
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
    // Simulation duration means only VMs with start time less than value are borrowed
    private static final double SIMULATION_LENGTH = 10000.0;

    public static void main(String[] args) throws Exception {
        int host_count = Integer.parseInt(args[0]);
        new VmTracesExample(host_count);
    }

    private VmTracesExample(int host_count) throws Exception {
        /*Enables just some level of log messages.
          Make sure to import org.cloudsimplus.util.Log;*/
        Log.setLevel(ch.qos.logback.classic.Level.INFO);

        long timeStart = System.currentTimeMillis();

        final CloudSim simulation = new CloudSim();
        final Datacenter dc = new DatacenterSimple(simulation, createHosts(host_count));
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        //Creates a broker that is a software acting on behalf of a cloud customer to manage his/her VMs
        DatacenterBroker broker = new DatacenterBrokerBestFit(simulation);

        var vmEvents = readVmEvents();
        var vmFinishTimes = new HashMap<String, String>();
        for (HuaweiDatasetVmEvent event: vmEvents) {
            final var vmId = event.vmId;
            final var isFinish = event.isFinish;
            final var time = event.time;

            if (isFinish) {
                vmFinishTimes.put(Integer.toString(vmId), Double.toString(time));
            }
        }

        ArrayList<Vm> vmList = new ArrayList();
        ArrayList<Cloudlet> cloudletList = new ArrayList();
        for (HuaweiDatasetVmEvent event: vmEvents) {
            final var vmId = event.vmId;
            final var vmPes = event.cpu;
            final var vmRam = event.memory;
            final var vmBw = 1000;
            final var vmSize = 1000;

            final var finishTimeOpt = Optional.ofNullable(vmFinishTimes.get(Integer.toString(event.vmId)));
            if (!finishTimeOpt.isPresent()) {
                continue;
            }
            if (event.time > SIMULATION_LENGTH || event.isFinish) {
                continue;
            }

            final var vm = new VmSimple(event.vmId, 1000, vmPes);
            final var duration = Double.parseDouble(finishTimeOpt.get()) - event.time;
            vm.setRam(vmRam).setBw(vmBw).setSize(vmSize).enableUtilizationStats();
            vm.setSubmissionDelay(event.time);
            vm.setStopTime(Double.parseDouble(finishTimeOpt.get()));
            vmList.add(vm);

            final var cloudlet = createCloudlet(vmId, vm, duration);
            cloudletList.add(cloudlet);
        }

        System.out.println("Number of VMs is " + vmList.size());

        broker.submitVmList(vmList);
        broker.submitCloudletList(cloudletList);

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
            peList.add(new PeSimple(1000));
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

    private ArrayList<HuaweiDatasetVmEvent> readVmEvents() throws Exception {
        ArrayList<HuaweiDatasetVmEvent> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(TRACES_PATH))) {
            String line;
            var line_num = 0;
            while ((line = br.readLine()) != null) {
                if (line_num++ == 0) {
                    continue;
                } 
                String[] values = line.split(COMMA_DELIMITER);
                HuaweiDatasetVmEvent event = new HuaweiDatasetVmEvent(values);
                records.add(event);    
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
    private Cloudlet createCloudlet(final int id, final Vm vm, final double duration) {
        final long fileSize = 1;
        final long outputSize = 1;
        final long length = (long)duration; //in number of Million Instructions (MI)
        final int pesNumber = 1;
        final var utilizationModel = new UtilizationModelFull();

        return new CloudletSimple(id, length, pesNumber)
            .setFileSize(fileSize)
            .setOutputSize(outputSize)
            .setUtilizationModel(utilizationModel)
            .setVm(vm);
    }
}
