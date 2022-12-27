pub fn assert_float_eq(x: f64, y: f64, eps: f64) {
    assert!(x > y - eps && x < y + eps);
}
