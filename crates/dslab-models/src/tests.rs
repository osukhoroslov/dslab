use sugars::boxed;

use crate::fair_sharing::FairThroughputSharingModel;
use crate::fair_sharing_slow::SlowFairThroughputSharingModel;
use crate::model::ThroughputSharingModel;

struct ModelsTester {
    fast_model: FairThroughputSharingModel<u32>,
    slow_model: SlowFairThroughputSharingModel<u32>,
}

impl ModelsTester {
    fn with_fixed_throughput(bandwidth: f64) -> Self {
        Self {
            fast_model: FairThroughputSharingModel::with_fixed_throughput(bandwidth),
            slow_model: SlowFairThroughputSharingModel::with_fixed_throughput(bandwidth),
        }
    }

    pub fn with_dynamic_throughput(throughput_function: fn(usize) -> f64) -> Self {
        Self {
            fast_model: FairThroughputSharingModel::with_dynamic_throughput(boxed!(throughput_function)),
            slow_model: SlowFairThroughputSharingModel::with_dynamic_throughput(boxed!(throughput_function)),
        }
    }

    fn insert_and_compare(&mut self, current_time: f64, volume: f64, item: u32) {
        self.fast_model.insert(current_time, volume, item);
        self.slow_model.insert(current_time, volume, item);
        assert_eq!(self.fast_model.peek(), self.slow_model.peek());
    }

    fn pop_all_and_compare(&mut self) -> Vec<(f64, u32)> {
        let mut fast_model_result = vec![];
        while let Some((time, item)) = self.fast_model.pop() {
            fast_model_result.push((time, item));
        }
        let mut slow_model_result = vec![];
        while let Some((time, item)) = self.slow_model.pop() {
            slow_model_result.push((time, item));
        }
        assert_eq!(fast_model_result, slow_model_result);
        return fast_model_result;
    }
}

#[test]
fn single_activity() {
    let mut te = ModelsTester::with_fixed_throughput(100.);
    te.insert_and_compare(0., 350., 0);
    assert_eq!(te.pop_all_and_compare(), vec![(3.5, 0)]);
}

#[test]
fn two_activities_with_simultaneous_start() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0., 150., 0);
    tester.insert_and_compare(0., 300., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 0), (4.5, 1)]);
}

#[test]
fn two_overlapping_activities() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0., 200., 0);
    tester.insert_and_compare(1., 200., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 0), (4., 1)]);
}

#[test]
fn complete_overlap() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0., 500., 0);
    tester.insert_and_compare(1., 100., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 1), (6., 0)]);
}

#[test]
fn correct_state_after_no_activities() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0., 500., 0);
    tester.insert_and_compare(1., 100., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 1), (6., 0)]);

    tester.insert_and_compare(10., 500., 0);
    tester.insert_and_compare(11., 100., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(13., 1), (16., 0)]);
}

#[test]
fn fractional_times() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0., 10., 0);
    tester.insert_and_compare(0.1, 90., 1);
    assert_eq!(tester.pop_all_and_compare(), vec![(0.1, 0), (1., 1)]);
}

#[test]
fn fairness() {
    let activities_count: usize = 5; // inserting more than 5 activities leads to problems with accuracy
    let mut tester = ModelsTester::with_fixed_throughput(1.);
    for i in 0..activities_count {
        let start_time = i as f64;
        tester.insert_and_compare(start_time, 1000., i as u32);
    }
    let result = tester.pop_all_and_compare();
    assert_eq!(result.len(), activities_count);
    for i in 0..activities_count {
        assert_eq!(result[i].1, i as u32);
    }
}

#[test]
fn equal_activities_ordering() {
    let activities_count: u32 = 7; // inserting more than 7 activities leads to problems with accuracy
    let mut tester = ModelsTester::with_fixed_throughput(activities_count as f64);
    let mut expected_result = vec![];
    for i in 0..activities_count {
        tester.insert_and_compare(0., activities_count as f64, i);
        expected_result.push((activities_count as f64, i));
    }
    assert_eq!(tester.pop_all_and_compare(), expected_result);
}

#[test]
fn dynamic_throughput() {
    fn throughput_function(n: usize) -> f64 {
        if n < 4 {
            100.
        } else {
            50.
        }
    }

    let mut tester = ModelsTester::with_dynamic_throughput(throughput_function);
    let mut expected_result = vec![];
    for i in 0..3 {
        tester.insert_and_compare(0., 100., i);
        expected_result.push((3., i));
    }
    assert_eq!(tester.pop_all_and_compare(), expected_result);

    tester = ModelsTester::with_dynamic_throughput(throughput_function);
    expected_result.clear();
    for i in 0..4 {
        tester.insert_and_compare(0., 100., i);
        expected_result.push((4. * 2., i));
    }
    assert_eq!(tester.pop_all_and_compare(), expected_result);
}
