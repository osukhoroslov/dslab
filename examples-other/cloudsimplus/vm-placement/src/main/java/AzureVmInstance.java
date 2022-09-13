import java.util.Optional;
import java.util.OptionalDouble;

public class AzureVmInstance {
    int vmId;
    String vmTypeId;
    double startTime;
    OptionalDouble endTime;

    public AzureVmInstance(String[] values) {
        this.vmId = Integer.parseInt(values[0]);
        this.vmTypeId = values[1];
        this.startTime = Double.parseDouble(values[2]);
        if (values[3] != "none" && values[3] != "") {
            this.endTime = OptionalDouble.of(Double.parseDouble(values[3]));
        } else {
            this.endTime = null;
        }
    }
}
