//! Coldstart policy implementation from "Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider" paper.
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use rand::prelude::*;

use crate::coldstart::{ColdStartPolicy, KeepaliveDecision};
use crate::container::Container;
use crate::extra::arima_extra::{arima_forecast, autofit};
use crate::function::Application;
use crate::invocation::Invocation;

const HEAD: f64 = 0.05;
const TAIL: f64 = 0.99;

struct ApplicationData {
    pub cv: f64,
    pub bin_len: f64,
    pub bins: Vec<usize>,
    pub sqsum: f64,
    pub sum: usize,
    pub oob: usize,
    pub raw: Vec<f64>,
}

impl ApplicationData {
    pub fn new(n_bins: usize, bin_len: f64) -> Self {
        Self {
            cv: 0.0,
            bin_len,
            bins: vec![0; n_bins],
            sqsum: 0.0,
            sum: 0,
            oob: 0,
            raw: Vec::new(),
        }
    }

    pub fn arima(&self) -> f64 {
        if self.raw.len() == 1 {
            return self.raw[0];
        }
        //println!("called arima with {} samples", self.raw.len());
        //arima sucks
        //pray that it doesn't crash (it can and it will)
        let (coeff, ar_order, _) = autofit(&self.raw, 1).unwrap();
        let mut ar = Vec::new();
        let mut ma = Vec::new();
        for c in &coeff[1..ar_order + 1] {
            ar.push(*c);
        }
        for c in &coeff[ar_order + 1..] {
            ma.push(*c);
        }
        arima_forecast(
            self.raw.as_slice(),
            1,
            Some(ar.as_slice()),
            Some(ma.as_slice()),
            1,
            &|_x, _y| 0.0,
            &mut thread_rng(),
        )
        .unwrap()[0]
    }

    pub fn get_head(&self) -> usize {
        self.get_percentile(HEAD)
    }

    pub fn get_percentile(&self, p: f64) -> usize {
        let mut prefix = 0;
        for (i, x) in self.bins.iter().enumerate() {
            prefix += x;
            if (prefix as f64) / (self.sum as f64) >= p {
                return i;
            }
        }
        self.bins.len() - 1
    }

    pub fn get_tail(&self) -> usize {
        self.get_percentile(TAIL)
    }

    pub fn oob_rate(&self) -> f64 {
        if self.sum == 0 && self.oob == 0 {
            0.0
        } else {
            (self.oob as f64) / ((self.oob + self.sum) as f64)
        }
    }

    pub fn update(&mut self, val: f64) {
        self.raw.push(val);
        let bin_id = (val / self.bin_len).floor() as usize;
        if bin_id < self.bins.len() {
            let mut mean = (self.sum as f64) / (self.bins.len() as f64);
            self.sqsum -= ((self.bins[bin_id] as f64) - mean) * ((self.bins[bin_id] as f64) - mean);
            self.bins[bin_id] += 1;
            self.sum += 1;
            mean = (self.sum as f64) / (self.bins.len() as f64);
            self.sqsum += ((self.bins[bin_id] as f64) - mean) * ((self.bins[bin_id] as f64) - mean);
            let std = (self.sqsum / ((self.bins.len() - 1) as f64)).sqrt();
            self.cv = std / mean;
        } else {
            self.oob += 1;
        }
    }
}

/// Refer to <https://www.usenix.org/conference/atc20/presentation/shahrad>.
pub struct HybridHistogramPolicy {
    range: f64,
    arima_margin: f64,
    hist_margin: f64,
    bin_len: f64,
    cv_thr: f64,
    oob_thr: f64,
    n_bins: usize,
    data: HashMap<usize, ApplicationData>,
    last: HashMap<usize, f64>,
    already_set: HashMap<(usize, usize), f64>,
}

enum Pattern {
    Uncertain,
    Certain,
    OutOfBounds,
}

impl HybridHistogramPolicy {
    /// Creates new HybridHistogramPolicy.
    pub fn new(range: f64, bin_len: f64, cv_thr: f64, oob_thr: f64, arima_margin: f64, hist_margin: f64) -> Self {
        let n_bins = (range / bin_len).round() as usize;
        Self {
            range,
            arima_margin,
            hist_margin,
            bin_len,
            cv_thr,
            oob_thr,
            n_bins,
            data: HashMap::new(),
            last: HashMap::new(),
            already_set: HashMap::new(),
        }
    }

    /// Creates policy from a map of strings containing policy parameters.
    pub fn from_options_map(options: &HashMap<String, String>) -> Self {
        let range = options.get("range").unwrap().parse::<f64>().unwrap();
        let bin_len = options
            .get("bin_len")
            .map(|s| s.parse::<f64>().unwrap())
            .unwrap_or(60.0);
        let cv_thr = options.get("cv_thr").map(|s| s.parse::<f64>().unwrap()).unwrap_or(2.0);
        let oob_thr = options.get("oob_thr").map(|s| s.parse::<f64>().unwrap()).unwrap_or(0.5);
        let arima_margin = options
            .get("arima_margin")
            .map(|s| s.parse::<f64>().unwrap())
            .unwrap_or(0.15);
        let hist_margin = options
            .get("hist_margin")
            .map(|s| s.parse::<f64>().unwrap())
            .unwrap_or(0.1);
        Self::new(range, bin_len, cv_thr, oob_thr, arima_margin, hist_margin)
    }

    fn describe_pattern(&mut self, app_id: usize) -> Pattern {
        let cv_thr = self.cv_thr;
        let oob_thr = self.oob_thr;
        let data = self.get_app(app_id);
        if data.oob_rate() >= oob_thr {
            Pattern::OutOfBounds
        } else if data.cv < cv_thr {
            Pattern::Uncertain
        } else {
            Pattern::Certain
        }
    }

    fn get_app(&mut self, id: usize) -> &ApplicationData {
        if let Entry::Vacant(e) = self.data.entry(id) {
            e.insert(ApplicationData::new(self.n_bins, self.bin_len));
        }
        self.data.get(&id).unwrap()
    }

    fn get_app_mut(&mut self, id: usize) -> &mut ApplicationData {
        if let Entry::Vacant(e) = self.data.entry(id) {
            e.insert(ApplicationData::new(self.n_bins, self.bin_len));
        }
        self.data.get_mut(&id).unwrap()
    }
}

impl ColdStartPolicy for HybridHistogramPolicy {
    fn keepalive_decision(&mut self, container: &Container) -> KeepaliveDecision {
        if let Some(t) = self.already_set.get(&(container.host_id, container.id)) {
            if t - container.last_change <= 0.0 {
                // last_change should be equal to current time
                return KeepaliveDecision::TerminateNow;
            } else {
                return KeepaliveDecision::OldWindow;
            }
        }
        let keepalive = match self.describe_pattern(container.app_id) {
            Pattern::Uncertain => {
                self.already_set
                    .insert((container.host_id, container.id), container.last_change + self.range);
                self.range
            }
            Pattern::Certain => {
                let tail = 1 + self.get_app(container.app_id).get_tail();
                (tail as f64) * self.bin_len * (1. + self.hist_margin)
            }
            Pattern::OutOfBounds => self.get_app(container.app_id).arima() * self.arima_margin * 2.,
        };
        KeepaliveDecision::NewWindow(keepalive)
    }

    fn prewarm_window(&mut self, app: &Application) -> f64 {
        match self.describe_pattern(app.id) {
            Pattern::Uncertain => 0.0,
            Pattern::Certain => {
                let head = self.get_app(app.id).get_head();
                (head as f64) * self.bin_len * (1. - self.hist_margin)
            }
            Pattern::OutOfBounds => self.get_app(app.id).arima() * (1. - self.arima_margin),
        }
    }

    fn update(&mut self, invocation: &Invocation, app: &Application) {
        let fn_id = invocation.func_id;
        if let Some(old) = self.last.get(&fn_id) {
            let it = f64::max(0.0, invocation.arrival_time - old);
            self.get_app_mut(app.id).update(it);
        }
        self.last.insert(fn_id, invocation.finish_time.unwrap());
    }

    fn to_string(&self) -> String {
        format!("HybridHistogramPolicy[range={:.2},bin_len={:.2},cv_thr={:.2},oob_thr={:.2},arima_margin={:.2},hist_margin={:.2}]", self.range, self.bin_len, self.cv_thr, self.oob_thr, self.arima_margin, self.hist_margin)
    }
}
