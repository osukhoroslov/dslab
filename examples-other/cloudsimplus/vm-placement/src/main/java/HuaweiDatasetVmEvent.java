public class HuaweiDatasetVmEvent {
    public int vmId;
    public int cpu;
    public long memory;
    public double time;
    public boolean isFinish;

    public HuaweiDatasetVmEvent(String[] values) {
        this.vmId = Integer.parseInt(values[0]);
        this.cpu = Integer.parseInt(values[1]);
        this.memory = Long.parseLong(values[2]);
        this.time = Double.parseDouble(values[3]);
        this.isFinish = (Integer.parseInt(values[4]) == 1);
    }
}
