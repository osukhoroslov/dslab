use crate::arima_extra::{arima_forecast, autofit};

use rand::prelude::*;

use serverless::coldstart::ColdStartPolicy;
use serverless::container::Container;
use serverless::function::Group;
use serverless::invoker::Invocation;

use std::collections::HashMap;

const HEAD: f64 = 0.05;
const TAIL: f64 = 0.99;

struct GroupData {
    pub cv: f64,
    pub bin_len: f64,
    pub bins: Vec<usize>,
    pub sqsum: f64,
    pub sum: usize,
    pub oob: usize,
    pub raw: Vec<f64>,
}

impl GroupData {
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
        for i in 1..ar_order + 1 {
            ar.push(coeff[i]);
        }
        for i in ar_order + 1..coeff.len() {
            ma.push(coeff[i]);
        }
        arima_forecast(
            self.raw.as_slice(),
            1,
            Some(ar.as_slice()),
            Some(ma.as_slice()),
            1,
            &|x, y| 0.0,
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

pub struct HybridHistogramPolicy {
    range: f64,
    arima_margin: f64,
    hist_margin: f64,
    bin_len: f64,
    cv_thr: f64,
    oob_thr: f64,
    n_bins: usize,
    data: HashMap<u64, GroupData>,
    last: HashMap<u64, f64>,
}

enum Pattern {
    Uncertain,
    Certain,
    OOB,
}

impl HybridHistogramPolicy {
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
        }
    }

    fn describe_pattern(&mut self, group_id: u64) -> Pattern {
        let cv_thr = self.cv_thr;
        let oob_thr = self.oob_thr;
        let data = self.get_group(group_id);
        if data.oob_rate() >= oob_thr {
            Pattern::OOB
        } else if data.cv < cv_thr {
            Pattern::Uncertain
        } else {
            Pattern::Certain
        }
    }

    fn get_group(&mut self, id: u64) -> &GroupData {
        if !self.data.contains_key(&id) {
            self.data.insert(id, GroupData::new(self.n_bins, self.bin_len));
        }
        self.data.get(&id).unwrap()
    }

    fn get_group_mut(&mut self, id: u64) -> &mut GroupData {
        if !self.data.contains_key(&id) {
            self.data.insert(id, GroupData::new(self.n_bins, self.bin_len));
        }
        self.data.get_mut(&id).unwrap()
    }
}

impl ColdStartPolicy for HybridHistogramPolicy {
    fn keepalive_window(&mut self, container: &Container) -> f64 {
        match self.describe_pattern(container.group_id) {
            Pattern::Uncertain => self.range,
            Pattern::Certain => {
                let tail = 1 + self.get_group(container.group_id).get_tail();
                (tail as f64) * self.bin_len * (1. + self.hist_margin)
            }
            Pattern::OOB => self.get_group(container.group_id).arima() * self.arima_margin * 2.,
        }
    }

    fn prewarm_window(&mut self, group: &Group) -> f64 {
        match self.describe_pattern(group.id) {
            Pattern::Uncertain => 0.0,
            Pattern::Certain => {
                let head = self.get_group(group.id).get_head();
                (head as f64) * self.bin_len * (1. - self.hist_margin)
            }
            Pattern::OOB => self.get_group(group.id).arima() * (1. - self.arima_margin),
        }
    }

    fn update(&mut self, invocation: &Invocation, group: &Group) {
        let fn_id = invocation.request.id;
        if let Some(old) = self.last.get(&fn_id) {
            let it = f64::max(0.0, invocation.request.time - old);
            self.get_group_mut(group.id).update(it);
        }
        self.last.insert(fn_id, invocation.finished.unwrap());
    }
}
