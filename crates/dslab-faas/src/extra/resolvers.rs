/// This file contains additional resolvers for YAML configs
use crate::coldstart::{default_coldstart_policy_resolver, ColdStartPolicy};
use crate::config::parse_options;
use crate::extra::hermes::HermesScheduler;
use crate::extra::hybrid_histogram::HybridHistogramPolicy;
use crate::scheduler::{default_scheduler_resolver, Scheduler};

pub fn extra_coldstart_policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    if s.len() >= 23 && &s[0..22] == "HybridHistogramPolicy[" && s.ends_with(']') {
        let opts = parse_options(&s[22..s.len() - 1]);
        return Box::new(HybridHistogramPolicy::from_options_map(&opts));
    }
    default_coldstart_policy_resolver(s)
}

pub fn extra_scheduler_resolver(s: &str) -> Box<dyn Scheduler> {
    if s == "HermesScheduler" {
        return Box::new(HermesScheduler::new());
    }
    default_scheduler_resolver(s)
}
