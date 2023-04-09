//! Tests hard_drive power models.

use crate::power::hard_drive::HardDriveState;
use crate::power::hard_drive_models::constant::ConstantPowerModel;
use crate::power::hard_drive_models::state_wise::StateWisePowerModel;
use crate::power::host::HostPowerModel;
use crate::power::host::HostState;

#[test]
fn test_constant_model() {
    let model = HostPowerModel::hdd_only(Box::new(ConstantPowerModel::new(0.5)));

    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Active)), 0.5);
    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Standby)), 0.5);
    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Idle)), 0.5);

    let state = HostState {
        cpu_util: None,
        memory_util: None,
        memory_read_util: None,
        memory_write_util: None,
        hdd_state: Some(HardDriveState::Active),
    };
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_state_wide_model() {
    let model = HostPowerModel::hdd_only(Box::new(StateWisePowerModel::new()));

    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Active)), 13.5);
    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Idle)), 10.2);
    assert_eq!(model.get_power(HostState::hard_drive(HardDriveState::Standby)), 2.5);

    let state = HostState {
        cpu_util: None,
        memory_util: None,
        memory_read_util: None,
        memory_write_util: None,
        hdd_state: Some(HardDriveState::Active),
    };
    assert_eq!(model.get_power(state), 13.5);
}
