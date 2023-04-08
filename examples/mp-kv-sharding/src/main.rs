use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use decorum::R64;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::Deserialize;
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// MESSAGES ------------------------------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Deserialize)]
struct PutRespMessage<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Deserialize)]
struct DeleteRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Deserialize)]
struct DumpKeysRespMessage {
    keys: HashSet<String>,
}

#[derive(Deserialize)]
struct CountRecordsRespMessage {
    count: u64,
}

// UTILS ---------------------------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    process_factory: &'a PyProcessFactory,
    proc_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig, measure_max_size: bool) -> System {
    let mut sys = System::new(config.seed);
    sys.network().set_delays(0.01, 0.1);
    let mut proc_names = Vec::new();
    for n in 0..config.proc_count {
        proc_names.push(format!("proc-{}", n));
    }
    for n in 0..config.proc_count {
        let proc_name = proc_names[n as usize].clone();
        let mut proc = config
            .process_factory
            .build((proc_name.clone(), proc_names.clone()), config.seed);
        if measure_max_size {
            proc.set_max_size_freq(1000000);
        }
        let node_name = format!("node-{}", n);
        sys.add_node(&node_name);
        sys.add_process(&proc_name, boxed!(proc), &node_name);
    }
    sys
}

fn add_node_and_process(name: &str, sys: &mut System, config: &TestConfig) {
    let proc_name = format!("proc-{}", name);
    let mut proc_names = sys.process_names();
    proc_names.push(proc_name.clone());
    let proc = config
        .process_factory
        .build((proc_name.clone(), proc_names), config.seed);
    let node_name = format!("node-{}", name);
    sys.add_node(&node_name);
    sys.add_process(&proc_name, boxed!(proc), &node_name);
}

fn check_get(sys: &mut System, proc: &str, key: &str, expected: Option<&str>, max_steps: u32) -> TestResult {
    sys.send_local_message(proc, Message::new("GET", &format!(r#"{{"key": "{}"}}"#, key)));
    // sys.step_until_no_events();
    // let msgs = sys.read_local_messages(proc);
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("GET_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "GET_RESP")?;
    let data: GetRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, expected)?;
    Ok(true)
}

fn check_put(sys: &mut System, proc: &str, key: &str, value: &str, max_steps: u32) -> TestResult {
    sys.send_local_message(
        proc,
        Message::new("PUT", &format!(r#"{{"key": "{}", "value": "{}"}}"#, key, value)),
    );
    // sys.step_until_no_events();
    // let msgs = sys.read_local_messages(proc);
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, value)?;
    Ok(true)
}

fn check_delete(sys: &mut System, proc: &str, key: &str, expected: Option<&str>, max_steps: u32) -> TestResult {
    sys.send_local_message(proc, Message::new("DELETE", &format!(r#"{{"key": "{}"}}"#, key)));
    // sys.step_until_no_events();
    // let msgs = sys.read_local_messages(proc);
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("DELETE_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "DELETE_RESP")?;
    let data: DeleteRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, expected)?;
    Ok(true)
}

fn dump_keys(sys: &mut System, proc: &str) -> Result<HashSet<String>, String> {
    sys.send_local_message(proc, Message::new("DUMP_KEYS", "{}"));
    // sys.step_until_no_events();
    // let msgs = sys.read_local_messages(proc);
    let res = sys.step_until_local_message_max_steps(proc, 100);
    assume!(res.is_ok(), format!("DUMP_KEYS_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "DUMP_KEYS_RESP")?;
    let data: DumpKeysRespMessage = serde_json::from_str(&msg.data).unwrap();
    Ok(data.keys)
}

fn key_distribution(sys: &mut System) -> Result<HashMap<String, HashSet<String>>, String> {
    let mut dist = HashMap::new();
    for proc in sys.process_names() {
        dist.insert(proc.clone(), dump_keys(sys, &proc)?);
    }
    Ok(dist)
}

fn count_records(sys: &mut System, proc: &str) -> Result<u64, String> {
    sys.send_local_message(proc, Message::new("COUNT_RECORDS", "{}"));
    // sys.step_until_no_events();
    // let msgs = sys.read_local_messages(proc);
    let res = sys.step_until_local_message_max_steps(proc, 100);
    assume!(res.is_ok(), format!("COUNT_RECORDS_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "COUNT_RECORDS_RESP")?;
    let data: CountRecordsRespMessage = serde_json::from_str(&msg.data).unwrap();
    Ok(data.count)
}

fn send_proc_added(sys: &mut System, added: &str) {
    for proc in sys.process_names() {
        if sys.node_is_crashed(&sys.proc_node_name(&proc)) {
            continue;
        }
        sys.send_local_message(&proc, Message::new("NODE_ADDED", &format!(r#"{{"id": "{}"}}"#, added)));
    }
}

fn send_proc_removed(sys: &mut System, removed: &str) {
    for proc in sys.process_names() {
        if sys.node_is_crashed(&sys.proc_node_name(&proc)) {
            continue;
        }
        sys.send_local_message(
            &proc,
            Message::new("NODE_REMOVED", &format!(r#"{{"id": "{}"}}"#, removed)),
        );
    }
}

fn step_until_stabilized(
    sys: &mut System,
    procs: &[String],
    expected_keys: u64,
    steps_per_iter: u64,
    max_steps: u64,
) -> TestResult {
    let mut stabilized = false;
    let mut steps = 0;
    let mut counts = HashMap::new();
    let mut total_count: u64 = 0;
    for proc in procs.iter() {
        let count = count_records(sys, proc)?;
        counts.insert(proc, count);
        total_count += count;
    }

    while !stabilized && steps <= max_steps {
        sys.steps(steps_per_iter);
        steps += steps_per_iter;
        total_count = 0;
        let mut count_changed = false;
        for proc in procs.iter() {
            let count = count_records(sys, proc)?;
            if *counts.get(proc).unwrap() != count {
                count_changed = true;
            }
            counts.insert(proc, count);
            total_count += count;
        }
        if total_count == expected_keys && !count_changed {
            stabilized = true;
        }
    }

    assume!(
        stabilized,
        format!(
            "Keys distribution is not stabilized (keys observed = {}, expected = {})",
            total_count, expected_keys
        )
    )
}

fn check(
    sys: &mut System,
    procs: &[String],
    expected: &HashMap<String, String>,
    check_values: bool,
    check_distribution: bool,
) -> TestResult {
    let mut stored_keys = HashSet::new();
    let mut proc_key_counts = Vec::new();
    for proc in procs.iter() {
        let proc_count = count_records(sys, proc)?;
        let proc_keys = dump_keys(sys, proc)?;
        assume_eq!(proc_keys.len() as u64, proc_count)?;
        stored_keys.extend(proc_keys);
        proc_key_counts.push(proc_count);
    }

    // all keys are stored
    assume!(
        expected.len() == stored_keys.len() && expected.keys().all(|k| stored_keys.contains(k)),
        "Stored keys do not mach expected"
    )?;

    // each key is stored on a single node
    assume!(
        proc_key_counts.iter().sum::<u64>() == stored_keys.len() as u64,
        "Keys are not stored on a single proc"
    )?;

    // check values
    if check_values {
        println!("\nChecking values:");
        for proc in procs.iter() {
            for (k, v) in expected.iter() {
                check_get(sys, proc, k, Some(v), 100)?;
            }
        }
        println!("OK")
    }

    // check keys distribution
    if check_distribution {
        let target_count = (expected.len() as f64 / proc_key_counts.len() as f64).round();
        let max_count = *proc_key_counts.iter().max().unwrap();
        let min_count = *proc_key_counts.iter().min().unwrap();
        let deviations: Vec<f64> = proc_key_counts
            .iter()
            .map(|x| (target_count - *x as f64).abs() / target_count)
            .collect();
        let avg_deviation = deviations.iter().sum::<f64>() / proc_key_counts.len() as f64;
        let max_deviation = deviations.iter().map(|x| R64::from_inner(*x)).max().unwrap();
        println!("\nStored keys per node:");
        println!("  - target: {}", target_count);
        println!("  - min: {}", min_count);
        println!("  - max: {}", max_count);
        println!("  - average deviation from target: {:.3}", avg_deviation);
        println!("  - max deviation from target: {:.3}", max_deviation);
        assume!(max_deviation <= 0.1, "Max deviation from target is above 10%")?;
    }

    Ok(true)
}

fn check_moved_keys(
    sys: &mut System,
    before: &HashMap<String, HashSet<String>>,
    after: &HashMap<String, HashSet<String>>,
    target: u64,
) -> TestResult {
    let mut total_count = 0;
    let mut not_moved_count = 0;
    let empty = HashSet::new();
    for proc in sys.process_names() {
        let b = before.get(&proc).unwrap_or(&empty);
        let a = after.get(&proc).unwrap_or(&empty);
        let not_moved: HashSet<String> = a.intersection(b).cloned().collect();
        not_moved_count += not_moved.len() as u64;
        total_count += b.len() as u64;
    }
    let moved_count = total_count - not_moved_count;
    let deviation = (moved_count as f64 - target as f64) / target as f64;
    println!("\nMoved keys:");
    println!("  - target: {}", target);
    println!("  - observed: {}", moved_count);
    println!("  - deviation: {:.3}", deviation);
    assume!(deviation <= 0.1, format!("Deviation from target is above 10%"))
}

const SYMBOLS: [char; 36] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
];
const WEIGHTS: [usize; 36] = [
    13, 16, 3, 8, 8, 5, 6, 23, 4, 8, 24, 12, 2, 1, 1, 10, 5, 8, 10, 1, 24, 3, 1, 8, 12, 22, 5, 20, 18, 5, 5, 2, 1, 3,
    16, 22,
];

fn random_string(length: usize, rand: &mut Pcg64) -> String {
    let dist = WeightedIndex::new(&WEIGHTS).unwrap();
    rand.sample_iter(&dist).take(length).map(|x| SYMBOLS[x]).collect()
}

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_single_node(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let proc = "proc-0";
    let key = random_string(8, &mut rand).to_uppercase();
    let value = random_string(8, &mut rand);
    let max_steps = 10;

    check_get(&mut sys, proc, &key, None, max_steps)?;
    check_put(&mut sys, proc, &key, &value, max_steps)?;
    check_get(&mut sys, proc, &key, Some(&value), max_steps)?;
    check_delete(&mut sys, proc, &key, Some(&value), max_steps)?;
    check_get(&mut sys, proc, &key, None, max_steps)?;
    check_delete(&mut sys, proc, &key, None, max_steps)
}

fn test_inserts(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs from each node
    let mut kv = HashMap::new();
    for proc in sys.process_names() {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // check that all key-values can be read from each node
    let procs = sys.process_names();
    check(&mut sys, &procs, &kv, true, false)
}

fn test_deletes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs from each node
    for proc in sys.process_names() {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // delete each key from one node and check that key is not present from another
    for (k, v) in kv.iter() {
        let read_proc = sys.process_names().choose(&mut rand).unwrap().clone();
        let mut delete_proc = sys.process_names().choose(&mut rand).unwrap().clone();
        while delete_proc == read_proc {
            delete_proc = sys.process_names().choose(&mut rand).unwrap().clone();
        }
        check_get(&mut sys, &read_proc, k, Some(v), 100)?;
        check_delete(&mut sys, &delete_proc, k, Some(v), 100)?;
        check_get(&mut sys, &read_proc, k, None, 100)?;
    }

    kv.clear();
    let procs = sys.process_names();
    check(&mut sys, &procs, &kv, false, false)
}

fn test_memory_overhead(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, true);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 10000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    let mut total_mem_size = 0;
    for proc in sys.process_names() {
        total_mem_size += sys.max_size(&proc)
    }
    let mem_size_per_key = total_mem_size as f64 / keys_count as f64;
    println!("Mem size per key: {}", mem_size_per_key);
    assume!(
        mem_size_per_key <= 300.,
        format!("Too big memory overhead (probably you use naive key->node mapping)")
    )
}

fn test_node_added(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new node to the system
    let added = sys.process_names().len().to_string();
    add_node_and_process(&added, &mut sys, config);
    send_proc_added(&mut sys, &format!("proc-{}", added));

    // run the system until key the distribution is stabilized
    let procs = sys.process_names();
    step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &procs, &kv, true, false)
}

fn test_node_removed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // remove a node from the system
    let removed = sys.process_names().choose(&mut rand).unwrap().clone();
    let count = count_records(&mut sys, &removed)?;
    assume!(count > 0, "Node stores no records, bad distribution")?;
    send_proc_removed(&mut sys, &removed);

    // run the system until key the distribution is stabilized
    let procs: Vec<String> = sys.process_names().into_iter().filter(|x| *x != removed).collect();
    step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &procs, &kv, true, false)
}

fn test_node_removed_after_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // crash a node and remove it from the system (stored keys are lost)
    let crashed = sys.process_names().choose(&mut rand).unwrap().clone();
    let crashed_keys = dump_keys(&mut sys, &crashed)?;
    assume!(!crashed_keys.is_empty(), "Proc stores no records, bad distribution")?;
    for k in crashed_keys {
        kv.remove(&k);
    }
    sys.crash_node(&sys.proc_node_name(&crashed));
    send_proc_removed(&mut sys, &crashed);

    // run the system until key the distribution is stabilized
    let procs: Vec<String> = sys.process_names().into_iter().filter(|x| *x != crashed).collect();
    step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &procs, &kv, true, false)
}

fn test_migration(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut procs = sys.process_names();

    // insert random key-value pairs
    let keys_count = 10000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new N nodes to the system
    for i in 0..config.proc_count {
        let added = format!("{}", config.proc_count + i);
        add_node_and_process(&added, &mut sys, config);
        send_proc_added(&mut sys, &format!("proc-{}", added));
        procs.push(format!("proc-{}", added));
        step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &procs, &kv, false, false)?;

    // remove old N nodes
    for _i in 0..config.proc_count {
        let removed = &procs[0_usize];
        send_proc_removed(&mut sys, removed);
        procs.remove(0);
        step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &procs, &kv, false, false)
}

fn test_scale_up_down(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut procs = sys.process_names();

    // insert random key-value pairs
    let keys_count = 1000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new N nodes to the system
    for i in 0..config.proc_count {
        let added = format!("{}", config.proc_count + i);
        add_node_and_process(&added, &mut sys, config);
        send_proc_added(&mut sys, &format!("proc-{}", added));
        procs.push(format!("proc-{}", added));
        step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &procs, &kv, false, false)?;

    // remove new N nodes
    for _i in 0..config.proc_count {
        let removed = &procs[config.proc_count as usize];
        send_proc_removed(&mut sys, removed);
        procs.remove(config.proc_count as usize);
        step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &procs, &kv, false, false)
}

fn test_distribution(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 10000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }

    let procs = sys.process_names();
    check(&mut sys, &procs, &kv, false, true)
}

fn test_distribution_node_added(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs
    let keys_count = 10000;
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }
    let dist_before = key_distribution(&mut sys)?;

    // add new node to the system
    let added = sys.process_names().len().to_string();
    add_node_and_process(&added, &mut sys, config);
    send_proc_added(&mut sys, &format!("proc-{}", added));

    // run the system until key the distribution is stabilized
    let procs = sys.process_names();
    step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    let dist_after = key_distribution(&mut sys)?;

    let target_moved_keys = (keys_count as f64 / procs.len() as f64).round() as u64;
    check_moved_keys(&mut sys, &dist_before, &dist_after, target_moved_keys)?;

    check(&mut sys, &procs, &kv, false, true)
}

fn test_distribution_node_removed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs
    let keys_count = 10000;
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let proc = sys.process_names().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &proc, &k, &v, 100)?;
        kv.insert(k, v);
    }
    let dist_before = key_distribution(&mut sys)?;

    // remove a node from the system
    let removed = sys.process_names().choose(&mut rand).unwrap().clone();
    let count = count_records(&mut sys, &removed)?;
    assume!(count > 0, "Node stores no records, bad distribution")?;
    send_proc_removed(&mut sys, &removed);

    // run the system until key the distribution is stabilized
    let procs: Vec<String> = sys.process_names().into_iter().filter(|x| *x != removed).collect();
    step_until_stabilized(&mut sys, &procs, kv.len() as u64, 100, 1000)?;
    let dist_after = key_distribution(&mut sys)?;

    let target_moved_keys = (keys_count as f64 / (procs.len() + 1) as f64).round() as u64;
    check_moved_keys(&mut sys, &dist_before, &dist_after, target_moved_keys)?;

    check(&mut sys, &procs, &kv, false, true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Sharded KV Store Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "python/solution.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Number of nodes used in tests
    #[clap(long, short, default_value = "10")]
    node_count: u32,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");

    let process_factory = PyProcessFactory::new(&args.solution_path, "StorageProcess");
    let config = TestConfig {
        process_factory: &process_factory,
        proc_count: args.node_count,
        seed: args.seed,
    };
    let mut single_config = config;
    single_config.proc_count = 1;
    let mut tests = TestSuite::new();

    tests.add("SINGLE NODE", test_single_node, single_config);
    tests.add("INSERTS", test_inserts, config);
    tests.add("DELETES", test_deletes, config);
    tests.add("MEMORY OVERHEAD", test_memory_overhead, config);
    tests.add("NODE ADDED", test_node_added, config);
    tests.add("NODE REMOVED", test_node_removed, config);
    tests.add("NODE REMOVED AFTER CRASH", test_node_removed_after_crash, config);
    tests.add("MIGRATION", test_migration, config);
    tests.add("SCALE UP DOWN", test_scale_up_down, config);
    tests.add("DISTRIBUTION", test_distribution, config);
    tests.add("DISTRIBUTION NODE ADDED", test_distribution_node_added, config);
    tests.add("DISTRIBUTION NODE REMOVED", test_distribution_node_removed, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
