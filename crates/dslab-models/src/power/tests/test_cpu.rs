//! Tests for CPU power models.

use crate::power::cpu::CpuPowerModel;
use crate::power::cpu_models::asymptotic::AsymptoticCpuPowerModel;
use crate::power::cpu_models::constant::ConstantCpuPowerModel;
use crate::power::cpu_models::cubic::CubicCpuPowerModel;
use crate::power::cpu_models::dvfs::DvfsAwareCpuPowerModel;
use crate::power::cpu_models::empirical::EmpiricalCpuPowerModel;
use crate::power::cpu_models::linear::LinearCpuPowerModel;
use crate::power::cpu_models::mse::MseCpuPowerModel;
use crate::power::cpu_models::square::SquareCpuPowerModel;
use crate::power::cpu_models::state_based::StateBasedCpuPowerModel;
use crate::power::hdd_models::constant::ConstantHddPowerModel;
use crate::power::host::HostPowerModel;
use crate::power::host::HostState;
use crate::power::memory_models::constant::ConstantMemoryPowerModel;
use std::collections::HashMap;

#[test]
fn test_mse_model() {
    let model = HostPowerModel::cpu_only(Box::new(MseCpuPowerModel::new(1., 0.4, 1.4)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.4);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.4 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.4);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.77);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.78);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.);
}

#[test]
fn test_square_model() {
    let model = HostPowerModel::cpu_only(Box::new(SquareCpuPowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.4);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.4 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.4);

    assert_eq!(model.get_power(HostState::cpu(0.5)), 0.55);

    assert!(model.get_power(HostState::cpu(0.8)) > 0.78);
    assert!(model.get_power(HostState::cpu(0.8)) < 0.79);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.);
}

#[test]
fn test_cubic_model() {
    let model = HostPowerModel::cpu_only(Box::new(CubicCpuPowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.4);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.4 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.4);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.47);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.48);

    assert!(model.get_power(HostState::cpu(0.8)) > 0.7);
    assert!(model.get_power(HostState::cpu(0.8)) < 0.71);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.);
}

#[test]
fn test_asymptotic_model() {
    let model = HostPowerModel::cpu_only(Box::new(AsymptoticCpuPowerModel::new(1., 0.4, 0.1)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.4);

    assert!(model.get_power(HostState::cpu(1e-5)) > 0.4);
    assert!(model.get_power(HostState::cpu(1e-5)) < 0.41);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.84);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.85);

    assert!(model.get_power(HostState::cpu(0.8)) > 0.93);
    assert!(model.get_power(HostState::cpu(0.8)) < 0.94);

    assert!(model.get_power(HostState::cpu(1.)) > 0.99);
    assert!(model.get_power(HostState::cpu(1.)) < 1.);
}

#[test]
fn test_empirical_model() {
    let utils = vec![0., 0.45, 0.51, 0.59, 0.64, 0.75, 0.79, 0.82, 0.91, 0.98, 1.];
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalCpuPowerModel::new(utils)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.);
    assert!((model.get_power(HostState::cpu(0.05)) - 0.225).abs() < 1e-12);
    assert_eq!(model.get_power(HostState::cpu(0.1)), 0.45);
    assert_eq!(model.get_power(HostState::cpu(0.89)), 0.973);
    assert_eq!(model.get_power(HostState::cpu(0.9)), 0.98);
    assert_eq!(model.get_power(HostState::cpu(0.99)), 0.998);
    assert_eq!(model.get_power(HostState::cpu(1.)), 1.);
}

#[test]
fn test_x3550_m3_xeon_x5675() {
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalCpuPowerModel::system_x3550_m3_xeon_x5675()));

    assert_eq!(model.get_power(HostState::cpu(0.)), 58.4);
    assert_eq!(model.get_power(HostState::cpu(0.1)), 98.);
    assert_eq!(model.get_power(HostState::cpu(0.85)), 197.);
    assert_eq!(model.get_power(HostState::cpu(1.)), 222.);
}

#[test]
fn test_dvfs_aware_model() {
    let model = HostPowerModel::cpu_only(Box::new(DvfsAwareCpuPowerModel::new(0.4, 0.4, 0.2)));
    let state = HostState::cpu_util_freq(0.5, 0.75);
    assert_eq!(model.get_power(state), 0.675);
}

#[test]
fn test_state_based_model() {
    let mut state_models: HashMap<&str, Box<dyn CpuPowerModel>> = HashMap::new();
    state_models.insert("C2", Box::new(ConstantCpuPowerModel::new(5.)));
    state_models.insert("C1", Box::new(ConstantCpuPowerModel::new(10.)));
    state_models.insert("P1", Box::new(LinearCpuPowerModel::new(20., 40.)));
    state_models.insert("P2", Box::new(LinearCpuPowerModel::new(40., 60.)));
    state_models.insert("P3", Box::new(LinearCpuPowerModel::new(60., 100.)));

    let model = HostPowerModel::cpu_only(Box::new(StateBasedCpuPowerModel::new(state_models, "P3")));

    // Test when the current CPU state is None (should use default state)
    let state = HostState::new(/*cpu_util*/ Some(0.5), None, None, None, None, None, None);
    assert_eq!(model.get_power(state), 80.);

    // Test when the current CPU state is unknown (should use default state)
    let state = HostState::cpu_util_state(0.4, "C0");
    assert_eq!(model.get_power(state), 76.);

    // Test P-states
    let state = HostState::cpu_util_state(0.6, "P3");
    assert_eq!(model.get_power(state), 84.);
    let state = HostState::cpu_util_state(0.6, "P2");
    assert_eq!(model.get_power(state), 52.);
    let state = HostState::cpu_util_state(0.6, "P1");
    assert_eq!(model.get_power(state), 32.);

    // Test C-states
    let state = HostState::cpu_util_state(0., "C1");
    assert_eq!(model.get_power(state), 10.);
    let state = HostState::cpu_util_state(0., "C2");
    assert_eq!(model.get_power(state), 5.);
}

#[test]
fn test_other_power() {
    let model = HostPowerModel::new(
        Box::new(MseCpuPowerModel::new(1., 0.4, 1.4)),
        Box::new(ConstantMemoryPowerModel::new(0.)),
        Box::new(ConstantHddPowerModel::new(0.)),
        0.2,
    );

    assert!((model.get_power(HostState::cpu(0.)) - 0.6).abs() < 1e-12);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.6 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.6);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.97);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.98);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.2);
}
