/// This file contains additional resolvers for YAML configs
use crate::coldstart::ColdStartPolicy;
use crate::config::{parse_options, stub_coldstart_policy_resolver};
use crate::extra::hermes::HermesScheduler;
use crate::extra::hybrid_histogram::HybridHistogramPolicy;
use crate::extra::simple_schedulers::*;
use crate::scheduler::{BasicScheduler, Scheduler};

pub fn extra_coldstart_policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    if s.len() >= 23 && &s[0..22] == "HybridHistogramPolicy[" && s.chars().next_back().unwrap() == ']' {
        let opts = parse_options(&s[22..s.len() - 1]);
        let range = opts.get("range").unwrap().parse::<f64>().unwrap();
        let bin_len = opts.get("bin_len").map(|s| s.parse::<f64>().unwrap()).unwrap_or(60.0);
        let cv_thr = opts.get("cv_thr").map(|s| s.parse::<f64>().unwrap()).unwrap_or(2.0);
        let oob_thr = opts.get("oob_thr").map(|s| s.parse::<f64>().unwrap()).unwrap_or(0.5);
        let arima_margin = opts
            .get("arima_margin")
            .map(|s| s.parse::<f64>().unwrap())
            .unwrap_or(0.15);
        let hist_margin = opts
            .get("hist_margin")
            .map(|s| s.parse::<f64>().unwrap())
            .unwrap_or(0.1);
        return Box::new(HybridHistogramPolicy::new(
            range,
            bin_len,
            cv_thr,
            oob_thr,
            arima_margin,
            hist_margin,
        ));
    }
    stub_coldstart_policy_resolver(s)
}

pub fn extra_scheduler_resolver(s: &str) -> Box<dyn Scheduler> {
    if s == "BasicScheduler" {
        return Box::new(BasicScheduler {});
    }
    if s == "HermesScheduler" {
        return Box::new(HermesScheduler::new());
    }
    if s == "RoundRobinScheduler" {
        return Box::new(RoundRobinScheduler::new());
    }
    if s.len() >= 17 && &s[0..16] == "RandomScheduler[" && s.chars().next_back().unwrap() == ']' {
        let opts = parse_options(&s[16..s.len() - 1]);
        let seed = opts.get("seed").unwrap().parse::<u64>().unwrap();
        return Box::new(RandomScheduler::new(seed));
    }
    if s.len() >= 22 && &s[0..21] == "LeastLoadedScheduler[" && s.chars().next_back().unwrap() == ']' {
        let opts = parse_options(&s[21..s.len() - 1]);
        let prefer_warm = opts.get("prefer_warm").unwrap().parse::<bool>().unwrap();
        return Box::new(LeastLoadedScheduler::new(prefer_warm));
    }
    if s.len() >= 24 && &s[0..23] == "LocalityBasedScheduler[" && s.chars().next_back().unwrap() == ']' {
        // Currently it is impossible to set custom hasher in YAML!
        let opts = parse_options(&s[23..s.len() - 1]);
        let warm_only = opts.get("warm_only").unwrap().parse::<bool>().unwrap();
        let step = opts.get("step").map(|s| s.parse::<usize>().unwrap());
        return Box::new(LocalityBasedScheduler::new(None, step, warm_only));
    }
    panic!("Can't resolve: {}", s);
}
