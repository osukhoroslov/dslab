use sugars::boxed;

use dslab_core::{Simulation, SimulationContext};

use super::fair_fast::FairThroughputSharingModel;
use super::fair_slow::SlowFairThroughputSharingModel;
use super::functions::ActivityFactorFn;
use super::model::{make_constant_resource_throughput_fn, ThroughputSharingModel};

fn assert_float_eq(x: f64, y: f64, eps: f64) {
    assert!(
        (x - y).abs() < eps || (x.max(y) - x.min(y)) / x.min(y) < eps,
        "Values do not match: {:.15} vs {:.15}",
        x,
        y
    );
}

struct ModelsTester {
    fast_model: FairThroughputSharingModel<u32>,
    slow_model: SlowFairThroughputSharingModel<u32>,
    sim: Simulation,
    ctx: SimulationContext,
}

impl ModelsTester {
    fn with_fixed_throughput(bandwidth: f64) -> Self {
        let mut sim = Simulation::new(123);
        let ctx = sim.create_context("test");
        Self {
            fast_model: FairThroughputSharingModel::with_fixed_throughput(bandwidth),
            slow_model: SlowFairThroughputSharingModel::with_fixed_throughput(bandwidth),
            sim,
            ctx,
        }
    }

    pub fn with_dynamic_throughput(throughput_function: fn(usize) -> f64) -> Self {
        let mut sim = Simulation::new(123);
        let ctx = sim.create_context("test");
        Self {
            fast_model: FairThroughputSharingModel::with_dynamic_throughput(boxed!(throughput_function)),
            slow_model: SlowFairThroughputSharingModel::with_dynamic_throughput(boxed!(throughput_function)),
            sim,
            ctx,
        }
    }

    fn advance_time(&mut self, duration: f64) {
        self.sim.step_for_duration(duration);
    }

    fn insert_and_compare(&mut self, item: u32, volume: f64) {
        self.fast_model.insert(item, volume, &mut self.ctx);
        self.slow_model.insert(item, volume, &mut self.ctx);
        let fast_item = self.fast_model.peek().unwrap();
        let slow_item = self.slow_model.peek().unwrap();
        assert_float_eq(fast_item.0, slow_item.0, 1e-12);
        assert_eq!(fast_item.1, slow_item.1);
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
        println!();
        for i in 0..fast_model_result.len() {
            assert_float_eq(fast_model_result[i].0, slow_model_result[i].0, 1e-12);
            println!("{} {}", fast_model_result[i].0, slow_model_result[i].0);
            assert_eq!(fast_model_result[i].1, slow_model_result[i].1);
        }
        return fast_model_result;
    }
}

#[test]
fn single_activity() {
    let mut te = ModelsTester::with_fixed_throughput(100.);
    te.insert_and_compare(0, 350.);
    assert_eq!(te.pop_all_and_compare(), vec![(3.5, 0)]);
}

#[test]
fn two_activities_with_simultaneous_start() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0, 150.);
    tester.insert_and_compare(1, 300.);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 0), (4.5, 1)]);
}

#[test]
fn two_overlapping_activities() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0, 200.);
    tester.advance_time(1.);
    tester.insert_and_compare(1, 200.);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 0), (4., 1)]);
}

#[test]
fn complete_overlap() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0, 500.);
    tester.advance_time(1.);
    tester.insert_and_compare(1, 100.);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 1), (6., 0)]);
}

#[test]
fn correct_state_after_no_activities() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0, 500.);
    tester.advance_time(1.);
    tester.insert_and_compare(1, 100.);
    assert_eq!(tester.pop_all_and_compare(), vec![(3., 1), (6., 0)]);

    tester.advance_time(9.);
    tester.insert_and_compare(0, 500.);
    tester.advance_time(1.);
    tester.insert_and_compare(1, 100.);
    assert_eq!(tester.pop_all_and_compare(), vec![(13., 1), (16., 0)]);
}

#[test]
fn fractional_times() {
    let mut tester = ModelsTester::with_fixed_throughput(100.);
    tester.insert_and_compare(0, 10.);
    tester.advance_time(0.1);
    tester.insert_and_compare(1, 90.);
    assert_eq!(tester.pop_all_and_compare(), vec![(0.1, 0), (1., 1)]);
}

#[test]
fn fairness() {
    let activities_count: usize = 100;
    let mut tester = ModelsTester::with_fixed_throughput(1.);
    for i in 0..activities_count {
        tester.insert_and_compare(i as u32, 1000.);
        tester.advance_time(1.)
    }
    let result = tester.pop_all_and_compare();
    assert_eq!(result.len(), activities_count);
    for i in 0..activities_count {
        assert_eq!(result[i].1, i as u32);
    }
}

#[test]
fn equal_activities_ordering() {
    let activities_count: u32 = 100;
    let mut tester = ModelsTester::with_fixed_throughput(activities_count as f64);
    let mut expected_result = vec![];
    for i in 0..activities_count {
        tester.insert_and_compare(i, activities_count as f64);
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
        tester.insert_and_compare(i, 100.);
        expected_result.push((3., i));
    }
    assert_eq!(tester.pop_all_and_compare(), expected_result);

    tester = ModelsTester::with_dynamic_throughput(throughput_function);
    expected_result.clear();
    for i in 0..4 {
        tester.insert_and_compare(i, 100.);
        expected_result.push((4. * 2., i));
    }
    assert_eq!(tester.pop_all_and_compare(), expected_result);
}

struct TestThroughputFactorFunction {}

impl ActivityFactorFn<u32> for TestThroughputFactorFunction {
    fn get_factor(&mut self, item: &u32, _ctx: &mut SimulationContext) -> f64 {
        if *item == 0 {
            0.8
        } else {
            0.5
        }
    }
}

#[test]
fn throughput_factor() {
    let mut sim = Simulation::new(123);
    let mut ctx = sim.create_context("test");
    let tf = make_constant_resource_throughput_fn(100.);
    let mut model: FairThroughputSharingModel<u32> =
        FairThroughputSharingModel::new(tf, boxed!(TestThroughputFactorFunction {}));
    model.insert(0, 160., &mut ctx);
    sim.step_until_time(1.);
    model.insert(1, 100., &mut ctx);
    sim.step_until_time(2.);
    model.insert(2, 25., &mut ctx);
    assert_eq!(model.pop(), Some((3.5, 0)));
    assert_eq!(model.pop(), Some((3.5, 2)));
    assert_eq!(model.pop(), Some((4.5, 1)));
}

#[test]
fn throughput_factor_and_degradation() {
    fn throughput_function(n: usize) -> f64 {
        if n > 1 {
            80.
        } else {
            100.
        }
    }

    let mut sim = Simulation::new(123);
    let mut ctx = sim.create_context("test");
    let mut model: FairThroughputSharingModel<u32> =
        FairThroughputSharingModel::new(boxed!(throughput_function), boxed!(TestThroughputFactorFunction {}));
    model.insert(0, 160., &mut ctx);
    sim.step_until_time(1.);
    model.insert(1, 100., &mut ctx);
    sim.step_until_time(2.);
    model.insert(2, 25., &mut ctx);
    assert_eq!(model.pop(), Some((3.875, 2)));
    assert_eq!(model.pop(), Some((4.125, 0)));
    assert_eq!(model.pop(), Some((5.125, 1)));
}
