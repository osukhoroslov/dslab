//! Tests CPU power models.

use crate::power::cpu_models::asymptotic::AsymptoticPowerModel;
use crate::power::cpu_models::cubic::CubicPowerModel;
use crate::power::cpu_models::empirical::EmpiricalPowerModel;
use crate::power::cpu_models::mse::MsePowerModel;
use crate::power::cpu_models::square::SquarePowerModel;
use crate::power::host::HostPowerModel;
use crate::power::host::HostState;

#[test]
fn test_mse_model() {
    let model = HostPowerModel::cpu_only(Box::new(MsePowerModel::new(1., 0.4, 1.4)));

    assert_eq!(model.get_power(HostState::cpu(0.)), 0.4);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.4 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.4);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.77);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.78);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.);
}

#[test]
fn test_square_model() {
    let model = HostPowerModel::cpu_only(Box::new(SquarePowerModel::new(1., 0.4)));

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
    let model = HostPowerModel::cpu_only(Box::new(CubicPowerModel::new(1., 0.4)));

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
    let model = HostPowerModel::cpu_only(Box::new(AsymptoticPowerModel::new(1., 0.4, 0.1)));

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
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalPowerModel::new(utils)));

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
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalPowerModel::system_x3550_m3_xeon_x5675()));

    assert_eq!(model.get_power(HostState::cpu(0.)), 58.4);
    assert_eq!(model.get_power(HostState::cpu(0.1)), 98.);
    assert_eq!(model.get_power(HostState::cpu(0.85)), 197.);
    assert_eq!(model.get_power(HostState::cpu(1.)), 222.);
}

#[test]
fn test_other_power() {
    let model = HostPowerModel::new(
        Box::new(MsePowerModel::new(1., 0.4, 1.4)),
        Box::new(crate::power::memory_models::constant::ConstantPowerModel::new(0.)),
        Box::new(crate::power::hard_drive_models::constant::ConstantPowerModel::new(0.)),
        0.2,
    );

    assert!((model.get_power(HostState::cpu(0.)) - 0.6).abs() < 1e-12);

    assert!(model.get_power(HostState::cpu(1e-5)) < 0.6 + 1e-3);
    assert!(model.get_power(HostState::cpu(1e-5)) > 0.6);

    assert!(model.get_power(HostState::cpu(0.5)) > 0.97);
    assert!(model.get_power(HostState::cpu(0.5)) < 0.98);

    assert_eq!(model.get_power(HostState::cpu(1.)), 1.2);
}
