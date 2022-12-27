mod common;
use common::assert_float_eq;

use dslab_faas::stats::SampleMetric;

#[test]
fn test_quantiles_degenerate() {
    let mut m: SampleMetric = Default::default();
    let init = 111111.0;
    m.add(init);
    for q in &[0.0, 0.25, 0.5, 0.75, 1.0] {
        let val = m.quantile(*q);
        assert_float_eq(val, init, 1e-9);
    }
}

#[test]
fn test_quantiles_simple() {
    let mut m: SampleMetric = Default::default();
    for i in 0..11 {
        m.add(i as f64);
    }
    for q in &[0.0, 0.25, 0.5, 0.75, 1.0] {
        let val = m.quantile(*q);
        assert_float_eq(val, 10.0 * (*q), 1e-12);
    }
}

#[test]
fn test_simple_methods() {
    let mut m: SampleMetric = Default::default();
    for i in 0..111 {
        m.add(i as f64);
    }
    assert_float_eq(m.sum(), 6105.0, 1e-12);
    assert_float_eq(m.mean(), 55.0, 1e-12);
    assert_float_eq(m.min().unwrap(), 0.0, 1e-12);
    assert_float_eq(m.max().unwrap(), 110.0, 1e-12);
    assert_float_eq(m.biased_variance(), 1026.6666666666666, 1e-9);
    assert_float_eq(m.unbiased_variance(), 1036.0, 1e-9);
}
