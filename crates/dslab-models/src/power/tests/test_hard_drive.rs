//! Tests hard_drive power models.

use crate::power::hard_drive_models::constant::ConstantPowerModel;
use crate::power::host::HostPowerModel;
use crate::power::host::HostState;

#[test]
fn test_constant_model() {
    let model = HostPowerModel::hdd_only(Box::new(ConstantPowerModel::new(0.5)));

    assert_eq!(model.get_power(HostState::hard_drive(0.)), 0.5);
    assert_eq!(model.get_power(HostState::hard_drive(1e-5)), 0.5);
    assert_eq!(model.get_power(HostState::hard_drive(0.5)), 0.5);
    assert_eq!(model.get_power(HostState::hard_drive(1.)), 0.5);

    let state = HostState {
        cpu_util: None,
        memory_util: None,
        memory_read_util: None,
        memory_write_util: None,
        hdd_util: Some(0.45),
    };
    assert_eq!(model.get_power(state), 0.5);
}
