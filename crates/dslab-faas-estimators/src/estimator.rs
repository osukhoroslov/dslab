use std::boxed::Box;

use num::{Bounded, Num};

use dslab_faas::config::Config;
use dslab_faas::trace::Trace;

// This enum represents estimation of the optimal value of some metric
pub enum Estimation<T: Num + Bounded> {
    LowerBound(T),
    Exact(T),
    UpperBound(T),
}

pub trait Estimator {
    type EstimationType: Num + Bounded;
    fn estimate(&mut self, config: Config, trace: Box<dyn Trace>) -> Estimation<Self::EstimationType>;
}
