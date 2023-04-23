//! Tests for memory power models.

use crate::power::host::HostPowerModel;
use crate::power::host::HostState;
use crate::power::memory_models::constant::ConstantMemoryPowerModel;
use crate::power::memory_models::ddr3::Ddr3MemoryPowerModel;

#[test]
fn test_constant_model() {
    let model = HostPowerModel::memory_only(Box::new(ConstantMemoryPowerModel::new(0.5)));

    assert_eq!(model.get_power(HostState::memory(0.)), 0.5);
    assert_eq!(model.get_power(HostState::memory(1e-5)), 0.5);
    assert_eq!(model.get_power(HostState::memory(0.5)), 0.5);
    assert_eq!(model.get_power(HostState::memory(1.)), 0.5);

    let state = HostState::new(None, None, None, None, Some(1.), Some(1.), None);
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_ddr3_model() {
    let model = HostPowerModel::memory_only(Box::new(Ddr3MemoryPowerModel::new(240.)));

    assert!(model.get_power(HostState::memory(0.)) > 23.4);
    assert!(model.get_power(HostState::memory(0.)) < 23.41);

    assert!(model.get_power(HostState::memory(1e-5)) > 23.4);
    assert!(model.get_power(HostState::memory(1e-5)) < 23.41);

    assert!(model.get_power(HostState::memory(0.5)) > 56.69);
    assert!(model.get_power(HostState::memory(0.5)) < 56.71);

    assert!(model.get_power(HostState::memory(1.)) > 89.9);
    assert!(model.get_power(HostState::memory(1.)) < 90.1);

    let mut state = HostState::new(None, None, None, None, Some(0.), Some(0.), None);
    assert!(model.get_power(state.clone()) > 23.4);
    assert!(model.get_power(state.clone()) < 23.41);

    state = HostState::new(None, None, None, Some(0.5), Some(0.4), Some(0.8), None);
    assert!(model.get_power(state.clone()) > 62.27);
    assert!(model.get_power(state.clone()) < 62.29);
}

#[test]
fn test_micron_model_custom_memory() {
    let model = Box::new(Ddr3MemoryPowerModel::custom_model(240., 32., 8.));
    assert_eq!(model.modules_count(), 8);

    let host_model = HostPowerModel::memory_only(model);
    assert_eq!(host_model.get_power(HostState::memory(0.)), 16.64);
}
