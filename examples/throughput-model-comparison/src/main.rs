use std::iter::zip;
use std::time::Instant;

use num::bigint::Sign;
use num::rational::BigRational;
use num::BigInt;

use rand::prelude::*;
use rand_pcg::Pcg64;

use dslab_models::throughput_sharing::{
    FairThroughputSharingModel, SlowFairThroughputSharingModel, ThroughputSharingModel,
};

mod rational_model;
use rational_model::FairThroughputSharingModelRational;
mod old_model;

struct Transfer {
    start_time: u32,
    weight: u32,
}

fn run_new_model(transfers: &[Transfer]) -> Vec<f64> {
    let mut model = FairThroughputSharingModel::<usize>::with_fixed_throughput(1.);
    let mut transfers = transfers.iter().enumerate().collect::<Vec<_>>();
    transfers.sort_by(|a, b| a.1.start_time.cmp(&b.1.start_time));

    let mut result = vec![0.; transfers.len()];

    for (id, transfer) in transfers.into_iter() {
        loop {
            let first = model.peek();
            if first.is_none() || first.unwrap().0 > transfer.start_time as f64 {
                break;
            }
            let (time, id) = model.pop().unwrap();
            result[id] = time;
        }
        model.insert(transfer.start_time as f64, transfer.weight as f64, id);
    }

    loop {
        let item = model.pop();
        if item.is_none() {
            break;
        }
        let (time, id) = item.unwrap();
        result[id] = time;
    }

    result
}

fn run_old_model(transfers: &[Transfer]) -> Vec<f64> {
    let mut model = old_model::FairThroughputSharingModel::<usize>::with_fixed_throughput(1.);
    let mut transfers = transfers.iter().enumerate().collect::<Vec<_>>();
    transfers.sort_by(|a, b| a.1.start_time.cmp(&b.1.start_time));

    let mut result = vec![0.; transfers.len()];

    for (id, transfer) in transfers.into_iter() {
        loop {
            let first = model.peek();
            if first.is_none() || first.unwrap().0 > transfer.start_time as f64 {
                break;
            }
            let (time, id) = model.pop().unwrap();
            result[id] = time;
        }
        model.insert(transfer.start_time as f64, transfer.weight as f64, id);
    }

    loop {
        let item = model.pop();
        if item.is_none() {
            break;
        }
        let (time, id) = item.unwrap();
        result[id] = time;
    }

    result
}

fn run_slow_model(transfers: &[Transfer]) -> Vec<f64> {
    let mut model = SlowFairThroughputSharingModel::<usize>::with_fixed_throughput(1.);
    let mut transfers = transfers.iter().enumerate().collect::<Vec<_>>();
    transfers.sort_by(|a, b| a.1.start_time.cmp(&b.1.start_time));

    let mut result = vec![0.; transfers.len()];

    for (id, transfer) in transfers.into_iter() {
        loop {
            let first = model.peek();
            if first.is_none() || first.unwrap().0 > transfer.start_time as f64 {
                break;
            }
            let (time, id) = model.pop().unwrap();
            result[id] = time;
        }
        model.insert(transfer.start_time as f64, transfer.weight as f64, id);
    }

    loop {
        let item = model.pop();
        if item.is_none() {
            break;
        }
        let (time, id) = item.unwrap();
        result[id] = time;
    }

    result
}

fn make_rat(x: u32) -> BigRational {
    BigRational::new(BigInt::new(Sign::Plus, vec![x]), BigInt::new(Sign::Plus, vec![1]))
}

fn approx_rational(x: BigRational) -> f64 {
    let x = x * make_rat(1000000000) * make_rat(1000000000);
    let (sign, v) = x.round().numer().to_u64_digits();
    let mut val: u128 = 0;
    for i in v.into_iter().rev() {
        val = (val << 64) + i as u128;
    }
    let mut val = val as i128;
    if sign == Sign::Minus {
        val = -val;
    }
    val as f64 / 1e18
}

fn run_rational_model(transfers: &[Transfer]) -> Vec<f64> {
    let mut model = FairThroughputSharingModelRational::<usize>::new(make_rat(1));
    let mut transfers = transfers.iter().enumerate().collect::<Vec<_>>();
    transfers.sort_by(|a, b| a.1.start_time.cmp(&b.1.start_time));

    let mut result = vec![make_rat(0); transfers.len()];

    for (i, (id, transfer)) in transfers.into_iter().enumerate() {
        eprint!("\r[1/3] {}     ", i);
        loop {
            let first = model.peek();
            if first.is_none() || first.unwrap().0 > make_rat(transfer.start_time as u32) {
                break;
            }
            let (time, id) = model.pop().unwrap();
            result[id] = time;
        }
        model.insert(make_rat(transfer.start_time), make_rat(transfer.weight), id);
    }

    loop {
        eprint!("\r[2/3] {}     ", model.len());
        let item = model.pop();
        if item.is_none() {
            break;
        }
        let (time, id) = item.unwrap();
        result[id] = time;
    }

    let result = result
        .into_iter()
        .enumerate()
        .map(|(i, x)| {
            eprint!("\r[3/3] {}     ", i);
            approx_rational(x)
        })
        .collect();
    eprint!("\r              \r");
    result
}

struct Stats {
    time: f64,
    name: String,
    max_abs_error: f64,
    max_rel_error: f64,
    mse: f64,
}

impl Stats {
    fn print_header() {
        let header = format!(
            "| {:10} | {:9} | {:13} | {:13} | {:13} |",
            "name", "time", "max_rel_error", "max_abs_error", "mse"
        );
        println!("{}", header);
        for c in header.chars() {
            if c == '|' {
                print!("|");
            } else {
                print!("-");
            }
        }
        println!();
    }

    fn print(&self) {
        println!(
            "| {:<10} | {:>9.3} | {:>13.3e} | {:>13.3e} | {:>13.3e} |",
            self.name, self.time, self.max_abs_error, self.max_rel_error, self.mse
        );
    }
}

fn get_stats(correct_values: &[f64], actual_values: &[f64]) -> Stats {
    let mut max_abs_error = 0_f64;
    let mut max_rel_error = 0_f64;
    let mut mse = 0_f64;

    for (correct, actual) in zip(correct_values.iter(), actual_values.iter()) {
        let abs_error = (correct - actual).abs();
        max_abs_error = max_abs_error.max(abs_error);
        let rel_error = if correct < &1. { abs_error } else { abs_error / correct };
        max_rel_error = max_rel_error.max(rel_error);

        mse += abs_error * abs_error;
    }

    mse /= correct_values.len() as f64;

    Stats {
        time: 0.,
        name: String::new(),
        max_abs_error,
        max_rel_error,
        mse,
    }
}

fn run_models(transfers: Vec<Transfer>) {
    println!("Running rational model...");
    let now = Instant::now();
    let res_rational = run_rational_model(&transfers);
    println!("Finished in {:.2?}", now.elapsed());

    // eprintln!("{:?}", res_rational);

    println!("Running new model...");
    let now = Instant::now();
    let res = run_new_model(&transfers);
    let elapsed = now.elapsed().as_millis();
    let mut new_fair = get_stats(&res_rational, &res);
    new_fair.time = elapsed as f64 / 1000.;
    new_fair.name = "new".to_string();

    println!("Running old model...");
    let now = Instant::now();
    let res = run_old_model(&transfers);
    let elapsed = now.elapsed().as_millis();
    let mut fair_stats = get_stats(&res_rational, &res);
    fair_stats.time = elapsed as f64 / 1000.;
    fair_stats.name = "old".to_string();

    println!("Running slow model...");
    let now = Instant::now();
    let res = run_slow_model(&transfers);
    let elapsed = now.elapsed().as_millis();
    let mut slow_fair_stats = get_stats(&res_rational, &res);
    slow_fair_stats.time = elapsed as f64 / 1000.;
    slow_fair_stats.name = "slow".to_string();

    Stats::print_header();
    new_fair.print();
    fair_stats.print();
    slow_fair_stats.print();
}

fn run_test(num_transfers: usize, min_time: u32, max_time: u32, min_weight: u32, max_weight: u32, rng: &mut Pcg64) {
    println!(
        "Running test with {} transfers with time in [{}; {}] and weight in [{}; {}]",
        num_transfers, min_time, max_time, min_weight, max_weight
    );

    let mut transfers: Vec<Transfer> = Vec::new();
    for _ in 0..num_transfers {
        transfers.push(Transfer {
            start_time: rng.gen_range(min_time..max_time),
            weight: rng.gen_range(min_weight..max_weight),
        });
    }

    run_models(transfers);
    println!();
}

fn run_benchmark(
    num_transfers: usize,
    min_time: u32,
    max_time: u32,
    min_weight: u32,
    max_weight: u32,
    rng: &mut Pcg64,
) {
    println!(
        "Running test with {} transfers with time in [{}; {}] and weight in [{}; {}]",
        num_transfers, min_time, max_time, min_weight, max_weight
    );

    let mut transfers: Vec<Transfer> = Vec::new();
    for _ in 0..num_transfers {
        transfers.push(Transfer {
            start_time: rng.gen_range(min_time..max_time),
            weight: rng.gen_range(min_weight..max_weight),
        });
    }
    println!("Running new model...");
    let now = Instant::now();
    let _res = run_new_model(&transfers);
    println!("Finished in {:.2?}", now.elapsed());

    println!("Running old model...");
    let now = Instant::now();
    let _res = run_old_model(&transfers);
    println!("Finished in {:.2?}", now.elapsed());
    println!();
}

fn main() {
    let mut rng = Pcg64::seed_from_u64(1);

    run_test(10, 0, 100, 1, 30, &mut rng);
    run_test(100, 0, 1000000, 1, 100000, &mut rng);
    run_test(1000, 0, 1000000, 1, 100000, &mut rng);
    run_test(5000, 0, 1000000, 1, 100000, &mut rng);
    run_test(10000, 0, 1000000, 1, 100000, &mut rng);
    // run_test(20000, 0, 1000000, 1, 100000, &mut rng); // ~20 minutes to run

    run_benchmark(100000, 0, 1000000, 1, 100000, &mut rng);
    run_benchmark(1000000, 0, 1000000, 1, 100000, &mut rng);
    run_benchmark(10000000, 0, 1000000, 1, 100000, &mut rng);
}
