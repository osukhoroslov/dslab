use dslab_power_models::power_model::ConstantPowerModel;
use dslab_power_models::power_model::HostPowerModel;

#[test]
fn test_many_components() {
    let model = HostPowerModel::new()
        .cpu_power_model(Box::new(ConstantPowerModel::new(1.)))
        .memory_power_model(Box::new(ConstantPowerModel::new(0.3)))
        .gpu_power_model(Box::new(ConstantPowerModel::new(0.2)));

    assert_eq!(model.get_power(0., 0.), 0.);
    assert_eq!(model.get_power(0., 1.), 1.5);
}
