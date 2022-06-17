use std::fmt::Debug;
use std::ops::{Add, AddAssign, Div};

use num::{Float, Num};
use rand::prelude::*;

// Copy-pasted from https://github.com/krfricke/arima/tree/master/src
// because latest arima crate doesn't have these features yet

pub(crate) fn autofit<T: Float + From<u32> + From<f64> + Into<f64> + Copy + Add + AddAssign + Div + Debug>(
    x: &[T],
    d: usize,
) -> Result<(Vec<f64>, usize, usize), arima::ArimaError> {
    let x: Vec<f64> = x.iter().map(|v| (*v).into()).collect();
    let n = x.len() as f64;
    let n_lags = 12;

    // Hardcoding for now
    // let alpha = 0.05;
    // ppf = scipy.stats.norm.ppf(1 - alpha / 2.0)
    let ppf = 1.959963984540054;

    // Estimate MA order
    // <https://www.statsmodels.org/devel/_modules/statsmodels/tsa/stattools.html#acf>
    let _acf = arima::acf::acf(&x, Some(n_lags), false).unwrap();
    let mult: Vec<f64> = _acf[1.._acf.len() - 1]
        .iter()
        .scan(0., |acc, v| {
            *acc += v.powf(2.);
            Some(1. + 2. * *acc)
        })
        .collect();
    let mut varacf = vec![0., 1. / n];
    let varacf_end: Vec<f64> = (0.._acf.len() - 2).map(|i| 1. / n * mult[i]).collect();
    varacf.extend(varacf_end);

    let interval: Vec<f64> = varacf.iter().map(|v| ppf * v.sqrt()).collect();
    let confint: Vec<(f64, f64)> = _acf.iter().zip(&interval).map(|(p, q)| (p - q, p + q)).collect();
    let bounds: Vec<(f64, f64)> = confint.iter().zip(&_acf).map(|((l, u), a)| (l - a, u - a)).collect();

    // Subtract one to compensate for the first value (lag=0)
    let ma_order = _acf
        .iter()
        .zip(bounds)
        .take_while(|(a, (l, u))| a < &l || a > &u)
        .count()
        - 1;

    // <https://www.statsmodels.org/devel/_modules/statsmodels/tsa/stattools.html#pacf>
    let _pacf = arima::acf::pacf(&x, Some(n_lags)).unwrap();
    let pacf_varacf = 1.0 / n;
    let pacf_interval = ppf * pacf_varacf.sqrt();
    let pacf_confint: Vec<(f64, f64)> = _pacf.iter().map(|p| (p - pacf_interval, p + pacf_interval)).collect();

    let pacf_bounds: Vec<(f64, f64)> = pacf_confint
        .iter()
        .zip(&_pacf)
        .map(|((l, u), a)| (l - a, u - a))
        .collect();

    // lag=0 isn't included so no need to subtract one
    let ar_order = _pacf
        .iter()
        .zip(pacf_bounds)
        .take_while(|(a, (l, u))| a < &l || a > &u)
        .count();

    match arima::estimate::fit(&x, ar_order, d, ma_order) {
        Ok(fitted) => Ok((fitted, ar_order, ma_order)),
        Err(e) => Err(e),
    }
}

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
