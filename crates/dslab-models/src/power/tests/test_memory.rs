//! Tests memory power models.

use crate::power::memory::linear::LinearPowerModel;
use crate::power::power_model::HostPowerModel;

#[test]
fn test_mse_model() {
    let model = HostPowerModel::memory_only(Box::new(LinearPowerModel::new(1.)));

    assert_eq!(model.get_power(0., 0., 0.), 0.);
    assert!(model.get_power(0., 0., 1e-5) < 1e-4);
    assert_eq!(model.get_power(0., 0., 0.5), 0.5);
    assert_eq!(model.get_power(0., 0.5, 0.), 0.);
    assert_eq!(model.get_power(0., 0., 1.), 1.);
}
