//! Tests CPU power models.

use crate::power::cpu::asymptotic::AsymptoticPowerModel;
use crate::power::cpu::cubic::CubicPowerModel;
use crate::power::cpu::empirical::EmpiricalPowerModel;
use crate::power::cpu::mse::MSEPowerModel;
use crate::power::cpu::square::SquarePowerModel;
use crate::power::power_model::HostPowerModel;

#[test]
fn test_mse_model() {
    let model = HostPowerModel::cpu_only(Box::new(MSEPowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(0.), 0.);

    assert!(model.get_power(1e-5) < 0.4 + 1e-3);
    assert!(model.get_power(1e-5) > 0.4);

    assert!(model.get_power(0.5) > 0.77);
    assert!(model.get_power(0.5) < 0.78);

    assert_eq!(model.get_power(1.), 1.);
}

#[test]
fn test_square_model() {
    let model = HostPowerModel::cpu_only(Box::new(SquarePowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(0.), 0.);

    assert!(model.get_power(1e-5) < 0.4 + 1e-3);
    assert!(model.get_power(1e-5) > 0.4);

    assert_eq!(model.get_power(0.5), 0.55);

    assert!(model.get_power(0.8) > 0.78);
    assert!(model.get_power(0.8) < 0.79);

    assert_eq!(model.get_power(1.), 1.);
}

#[test]
fn test_cubic_model() {
    let model = HostPowerModel::cpu_only(Box::new(CubicPowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(0.), 0.);

    assert!(model.get_power(1e-5) < 0.4 + 1e-3);
    assert!(model.get_power(1e-5) > 0.4);

    assert!(model.get_power(0.5) > 0.47);
    assert!(model.get_power(0.5) < 0.48);

    assert!(model.get_power(0.8) > 0.7);
    assert!(model.get_power(0.8) < 0.71);

    assert_eq!(model.get_power(1.), 1.);
}

#[test]
fn test_asymptotic_model() {
    let model = HostPowerModel::cpu_only(Box::new(AsymptoticPowerModel::new(1., 0.4)));

    assert_eq!(model.get_power(0.), 0.);

    assert!(model.get_power(1e-5) > 0.4);
    assert!(model.get_power(1e-5) < 0.41);

    assert!(model.get_power(0.5) > 0.84);
    assert!(model.get_power(0.5) < 0.85);

    assert!(model.get_power(0.8) > 0.93);
    assert!(model.get_power(0.8) < 0.94);

    assert!(model.get_power(1.) > 0.99);
    assert!(model.get_power(1.) < 1.);
}

#[test]
fn test_empirical_model() {
    let utils = vec![0., 0.45, 0.51, 0.59, 0.64, 0.75, 0.79, 0.82, 0.91, 0.98, 1.];
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalPowerModel::new(1., utils)));

    assert_eq!(model.get_power(0.), 0.);
    assert_eq!(model.get_power(0.05), 0.);
    assert_eq!(model.get_power(0.1), 0.45);
    assert_eq!(model.get_power(0.89), 0.91);
    assert_eq!(model.get_power(0.9), 0.98);
    assert_eq!(model.get_power(0.99), 0.98);
    assert_eq!(model.get_power(1.), 1.);
}

#[test]
fn test_xeon_x5675() {
    let model = HostPowerModel::cpu_only(Box::new(EmpiricalPowerModel::xeon_x5675()));

    assert_eq!(model.get_power(0.), 0.);
    assert_eq!(model.get_power(0.1), 97.68);
    assert_eq!(model.get_power(0.85), 188.7);
    assert_eq!(model.get_power(1.), 222.);
}

#[test]
fn test_other_power() {
    let model = HostPowerModel::cpu_and_other(Box::new(MSEPowerModel::new(1., 0.4)), 0.2);

    assert_eq!(model.get_power(0.), 0.2);

    assert!(model.get_power(1e-5) < 0.6 + 1e-3);
    assert!(model.get_power(1e-5) > 0.6);

    assert!(model.get_power(0.5) > 0.97);
    assert!(model.get_power(0.5) < 0.98);

    assert_eq!(model.get_power(1.), 1.2);
}
