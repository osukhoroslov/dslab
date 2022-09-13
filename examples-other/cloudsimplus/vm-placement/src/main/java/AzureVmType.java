public class AzureVmType {
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

