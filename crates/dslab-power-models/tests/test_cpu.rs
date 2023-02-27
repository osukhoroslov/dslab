use dslab_power_models::cpu::utilization_aware::UtilizationAwarePowerModel;
use dslab_power_models::power_model::HostPowerModel;

#[test]
fn test_utilization_aware_model() {
    let model = HostPowerModel::new().cpu_power_model(Box::new(UtilizationAwarePowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(0., 0.), 0.);

    assert!(model.get_power(0., 1e-5) < 0.4 + 1e-3);
    assert!(model.get_power(0., 1e-5) > 0.4);

    assert!(model.get_power(0., 0.5) > 0.77);
    assert!(model.get_power(0., 0.5) < 0.78);

    assert_eq!(model.get_power(0., 1.), 1.);
}
