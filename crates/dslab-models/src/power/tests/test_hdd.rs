//! Tests for HDD power models.

use crate::power::hdd::HddState;
use crate::power::hdd_models::constant::ConstantHddPowerModel;
use crate::power::hdd_models::state_based::StateBasedHddPowerModel;
use crate::power::host::HostPowerModel;
use crate::power::host::HostState;

#[test]
fn test_constant_model() {
    let model = HostPowerModel::hdd_only(Box::new(ConstantHddPowerModel::new(0.5)));

    assert_eq!(model.get_power(HostState::hdd(HddState::Active)), 0.5);
    assert_eq!(model.get_power(HostState::hdd(HddState::Standby)), 0.5);
    assert_eq!(model.get_power(HostState::hdd(HddState::Idle)), 0.5);

    let state = HostState::new(None, None, None, None, None, Some(HddState::Active));
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_state_wise_model() {
    let model = HostPowerModel::hdd_only(Box::new(StateBasedHddPowerModel::ibm_36z15()));

    assert_eq!(model.get_power(HostState::hdd(HddState::Active)), 13.5);
    assert_eq!(model.get_power(HostState::hdd(HddState::Idle)), 10.2);
    assert_eq!(model.get_power(HostState::hdd(HddState::Standby)), 2.5);

    let state = HostState::new(None, None, None, None, None, Some(HddState::Active));
    assert_eq!(model.get_power(state), 13.5);
}
