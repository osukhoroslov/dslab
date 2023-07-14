//! Tests for memory power models.

use approx::{assert_abs_diff_eq, assert_relative_eq};

use crate::power::host::{HostPowerModelBuilder, HostState};
use crate::power::memory_models::constant::ConstantMemoryPowerModel;
use crate::power::memory_models::ddr3::Ddr3MemoryPowerModel;

#[test]
fn test_constant_model() {
    let model = HostPowerModelBuilder::new()
        .memory(Box::new(ConstantMemoryPowerModel::new(0.5)))
        .build();

    let mut state = HostState {
        memory_util: Some(0.),
        ..Default::default()
    };
    assert_eq!(model.get_power(state), 0.5);
    state.memory_util = Some(1e-5);
    assert_eq!(model.get_power(state), 0.5);
    state.memory_util = Some(0.5);
    assert_eq!(model.get_power(state), 0.5);
    state.memory_util = Some(1.);
    assert_eq!(model.get_power(state), 0.5);

    let state = HostState {
        memory_read_util: Some(1.),
        memory_write_util: Some(1.),
        ..Default::default()
    };
    assert_eq!(model.get_power(state), 0.5);
}

#[test]
fn test_ddr3_model() {
    let model = HostPowerModelBuilder::new()
        .memory(Box::new(Ddr3MemoryPowerModel::new(240.)))
        .build();

    let mut state = HostState {
        memory_util: Some(0.),
        ..Default::default()
    };
    assert_relative_eq!(model.get_power(state), 23.4);

    state.memory_util = Some(0.1);
    assert_abs_diff_eq!(model.get_power(state), 30.06);

    state.memory_util = Some(0.5);
    assert_abs_diff_eq!(model.get_power(state), 56.7);

    state.memory_util = Some(1.);
    assert_abs_diff_eq!(model.get_power(state), 90.);

    let state = HostState {
        memory_read_util: Some(0.),
        memory_write_util: Some(0.),
        ..Default::default()
    };
    assert_relative_eq!(model.get_power(state), 23.4);

    let state = HostState {
        memory_util: Some(0.5),
        memory_read_util: Some(0.4),
        memory_write_util: Some(0.8),
        ..Default::default()
    };
    assert_abs_diff_eq!(model.get_power(state), 62.28);
}

#[test]
fn test_ddr3_custom_model() {
    let model = Box::new(Ddr3MemoryPowerModel::custom_model(240., 32., 8.));
    assert_eq!(model.modules_count(), 8);

    let host_model = HostPowerModelBuilder::new().memory(model).build();

    let mut state = HostState {
        memory_util: Some(0.),
        ..Default::default()
    };
    assert_abs_diff_eq!(host_model.get_power(state), 16.64);

    state.memory_util = Some(0.5);
    assert_abs_diff_eq!(host_model.get_power(state), 40.32);
}
