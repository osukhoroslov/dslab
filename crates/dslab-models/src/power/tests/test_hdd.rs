//! Tests for HDD power models.

use crate::power::hdd::HddState;
use crate::power::hdd_models::constant::ConstantHddPowerModel;
use crate::power::hdd_models::state_based::StateBasedHddPowerModel;
use crate::power::host::{HostPowerModelBuilder, HostState};

#[test]
fn test_constant_model() {
    let model = HostPowerModelBuilder::new()
        .hard_drive(Box::new(ConstantHddPowerModel::new(0.5)))
        .build();

    let mut state = HostState {
        hdd_state: Some(HddState::Active),
        ..Default::default()
    };
    assert_eq!(model.get_power(state), 0.5);

    state.hdd_state = Some(HddState::Idle);
    assert_eq!(model.get_power(state), 0.5);

    state.hdd_state = Some(HddState::Standby);
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_state_based_model() {
    let model = HostPowerModelBuilder::new()
        .hard_drive(Box::new(StateBasedHddPowerModel::ibm_36z15()))
        .build();

    let mut state = HostState {
        hdd_state: Some(HddState::Active),
        ..Default::default()
    };
    assert_eq!(model.get_power(state), 13.5);

    state.hdd_state = Some(HddState::Idle);
    assert_eq!(model.get_power(state), 10.2);

    state.hdd_state = Some(HddState::Standby);
    assert_eq!(model.get_power(state), 2.5);
}
