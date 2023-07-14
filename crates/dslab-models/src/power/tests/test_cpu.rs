//! Tests for CPU power models.

use approx::assert_abs_diff_eq;

use crate::power::cpu_models::asymptotic::AsymptoticCpuPowerModel;
use crate::power::cpu_models::constant::ConstantCpuPowerModel;
use crate::power::cpu_models::cubic::CubicCpuPowerModel;
use crate::power::cpu_models::dvfs::DvfsAwareCpuPowerModel;
use crate::power::cpu_models::empirical::EmpiricalCpuPowerModel;
use crate::power::cpu_models::linear::LinearCpuPowerModel;
use crate::power::cpu_models::mse::MseCpuPowerModel;
use crate::power::cpu_models::square::SquareCpuPowerModel;
use crate::power::cpu_models::state_based::StateBasedCpuPowerModel;
use crate::power::host::HostPowerModelBuilder;
use crate::power::host::HostState;

#[test]
fn test_constant_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(ConstantCpuPowerModel::new(0.99)))
        .build();
    assert_eq!(model.get_power(HostState::cpu_util(0.)), 0.99);
    assert_eq!(model.get_power(HostState::cpu_util(0.1)), 0.99);
    assert_eq!(model.get_power(HostState::cpu_util(0.5)), 0.99);
    assert_eq!(model.get_power(HostState::cpu_util(1.)), 0.99);
}

#[test]
fn test_linear_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(LinearCpuPowerModel::new(0.4, 1.)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.46);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.7);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_linear_model_with_idle_power() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(LinearCpuPowerModel::new(0.5, 1.)))
        .cpu_idle(0.4)
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.55);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.75);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_mse_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(MseCpuPowerModel::new(0.4, 1., 1.4)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.4961135697, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.7726425150, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_square_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(SquareCpuPowerModel::new(0.4, 1.)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.406);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.55);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.8)), 0.784);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_cubic_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(CubicCpuPowerModel::new(0.4, 1.)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.4006);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.475);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.8)), 0.7072);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_asymptotic_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(AsymptoticCpuPowerModel::new(0.4, 1., 0.1)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.6196361676, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.8479786159, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.8)), 0.9398993612, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 0.9999863800, epsilon = 1e-10);
}

#[test]
fn test_empirical_model() {
    let utils = vec![0., 0.45, 0.51, 0.59, 0.64, 0.75, 0.79, 0.82, 0.91, 0.98, 1.];
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(EmpiricalCpuPowerModel::new(utils)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.05)), 0.225);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.45);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.75);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.51)), 0.754);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.89)), 0.973);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.9)), 0.98);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.99)), 0.998);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.);
}

#[test]
fn test_x3550_m3_xeon_x5675() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(EmpiricalCpuPowerModel::system_x3550_m3_xeon_x5675()))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 58.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 98.);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.85)), 197.);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 222.);
}

#[test]
fn test_dvfs_aware_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(DvfsAwareCpuPowerModel::new(0.4, 0.4, 0.2)))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0., 0.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0., 1.)), 0.4);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0.1, 0.)), 0.44);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0.1, 0.5)), 0.45);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0.1, 1.)), 0.46);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0.5, 0.75)), 0.675);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(0.8, 0.75)), 0.84);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(1., 0.75)), 0.95);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_freq(1., 1.)), 1.);
}

#[test]
fn test_state_based_model() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(StateBasedCpuPowerModel::new(vec![
            Box::new(LinearCpuPowerModel::new(0.6, 1.)),    // state 0
            Box::new(LinearCpuPowerModel::new(0.55, 0.85)), // state 1
            Box::new(LinearCpuPowerModel::new(0.5, 0.75)),  // state 2
            Box::new(ConstantCpuPowerModel::new(0.45)),     // state 3
            Box::new(ConstantCpuPowerModel::new(0.4)),      // state 4
        ])))
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_state(0.6, 0)), 0.84);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_state(0.6, 1)), 0.73);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_state(0.6, 2)), 0.65);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_state(0., 3)), 0.45);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util_state(0., 4)), 0.4);
}

#[test]
fn test_other_power() {
    let model = HostPowerModelBuilder::new()
        .cpu(Box::new(MseCpuPowerModel::new(0.4, 1., 1.4)))
        .other(0.2)
        .build();
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.)), 0.6);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.1)), 0.6961135697, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(0.5)), 0.9726425150, epsilon = 1e-10);
    assert_abs_diff_eq!(model.get_power(HostState::cpu_util(1.)), 1.2);
}
