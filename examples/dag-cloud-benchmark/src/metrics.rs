const REFPT: f64 = 1.1;

pub fn hypervolume(mut pts: Vec<(f64, f64)>, reference: (f64, f64)) -> f64 {
    pts.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.total_cmp(&b.1).reverse()));
    let mut lowest: f64 = REFPT;
    let mut result = 0f64;
    for pt in &pts {
        let y = pt.1 / reference.1;
        if y < lowest {
            result += (lowest - y) * (REFPT - pt.0 / reference.0);
            lowest = y;
        }
    }
    result
}

pub fn coverage(a: &[(f64, f64)], b: &[(f64, f64)]) -> f64 {
    let mut cnt = 0;
    for s in b {
        for t in a {
            if s.0 >= t.0 && s.1 >= t.1 {
                cnt += 1;
                break;
            }
        }
    }
    (cnt as f64) / (b.len() as f64)
}
