//! Tests memory power models.

use crate::power::host::HostPowerModel;
use crate::power::host::HostState;
use crate::power::memory_models::constant::ConstantPowerModel;
use crate::power::memory_models::micron::MicronPowerModel;

#[test]
fn test_constant_model() {
    let model = HostPowerModel::memory_only(Box::new(ConstantPowerModel::new(0.5)));

    assert_eq!(model.get_power(HostState::memory(0.)), 0.5);
    assert_eq!(model.get_power(HostState::memory(1e-5)), 0.5);
    assert_eq!(model.get_power(HostState::memory(0.5)), 0.5);
    assert_eq!(model.get_power(HostState::memory(1.)), 0.5);

    let state = HostState {
        cpu_util: None,
        memory_util: None,
        memory_read_util: Some(1.),
        memory_write_util: Some(1.),
        hdd_state: None,
    };
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_micron_model() {
    let model = HostPowerModel::memory_only(Box::new(MicronPowerModel::new(240.)));

    assert!(model.get_power(HostState::memory(0.)) > 23.4);
    assert!(model.get_power(HostState::memory(0.)) < 23.41);

    assert!(model.get_power(HostState::memory(1e-5)) > 23.4);
    assert!(model.get_power(HostState::memory(1e-5)) < 23.41);

    assert!(model.get_power(HostState::memory(0.5)) > 56.69);
    assert!(model.get_power(HostState::memory(0.5)) < 56.71);

    assert!(model.get_power(HostState::memory(1.)) > 89.9);
    assert!(model.get_power(HostState::memory(1.)) < 90.1);

    let mut state = HostState {
        cpu_util: None,
        memory_util: None,
        memory_read_util: Some(0.),
        memory_write_util: Some(0.),
        hdd_state: None,
    };
    assert!(model.get_power(state.clone()) > 23.4);
    assert!(model.get_power(state.clone()) < 23.41);

    state = HostState {
        cpu_util: None,
        memory_util: Some(0.5),
        memory_read_util: Some(0.4),
        memory_write_util: Some(0.8),
        hdd_state: None,
    };
    assert!(model.get_power(state.clone()) > 62.27);
    assert!(model.get_power(state.clone()) < 62.29);
}

#[test]
fn test_micron_model_custom_memory() {
    let model = Box::new(MicronPowerModel::custom_model(240., 32., 8.));
    assert_eq!(model.sticks_count(), 8);

    let host_model = HostPowerModel::memory_only(model);
    assert_eq!(host_model.get_power(HostState::memory(0.)), 16.64);
}
