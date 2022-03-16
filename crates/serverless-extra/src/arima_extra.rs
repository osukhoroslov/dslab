use num::Num;

use rand::prelude::*;

use std::ops::{Add, AddAssign};

// Copy-pasted from https://github.com/krfricke/arima/tree/master/src
// because latest arima crate doesn't have this function yet
pub(crate) fn arima_forecast<F: Fn(usize, &mut T) -> f64, T: Rng>(
    ts: &[f64],
    n: usize,
    ar: Option<&[f64]>,
    ma: Option<&[f64]>,
    d: usize,
    noise_fn: &F,
    rng: &mut T,
) -> Result<Vec<f64>, arima::ArimaError> {
    let n_past = ts.len();
    let mut x = ts.to_vec();

    // get orders
    let ar_order = match ar {
        Some(par) => par.len(),
        None => 0 as usize,
    };
    let ma_order = match ma {
        Some(par) => par.len(),
        None => 0 as usize,
    };

    // initialize forecast with noise
    for i in 0..n {
        let e = noise_fn(i, rng);
        x.push(e);
    }

    // create further noise and calculate MA part
    if ma_order > 0 {
        let ma = ma.unwrap();
        let x_ = x.clone();
        for i in n_past..n_past + n {
            for j in 0..ma_order {
                x[i] += ma[j] * x_[i - j - 1];
            }
        }
    }

    // calculate AR part
    if ar_order > 0 {
        let ar = ar.unwrap();
        for i in n_past..n_past + n {
            for j in 0..ar_order {
                x[i] += ar[j] * x[i - j - 1];
            }
        }
    }

    // remove burn_in part from vector, calculate differences
    if d > 0 {
        x = diffinv(&x[n_past..x.len()], d);
        // drop the d zeros at the start
        x.drain(0..d);
    } else {
        x.drain(0..n_past);
    }

    Ok(x)
}

pub fn diffinv<T: Num + Add + AddAssign + Copy + From<u8>>(x: &[T], d: usize) -> Vec<T> {
    let zero = From::from(0);

    // x vector with d leading zeros
    let mut cum: Vec<T> = [&vec![zero; d], x].concat().to_vec();

    // build cumulative sum d times
    for _ in 0..d {
        cum = cumsum(&cum);
    }
    cum
}

pub fn cumsum<T: Num + Add + AddAssign + Copy + From<u8>>(x: &[T]) -> Vec<T> {
    let mut y: Vec<T> = Vec::new();
    if x.len() < 2 {
        y.push(From::from(0));
        return y;
    }
    y.push(x[0]);
    for i in 1..x.len() {
        y.push(y[i - 1] + x[i]);
    }
    y
}
