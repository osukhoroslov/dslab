use std::borrow::Cow;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::time::Duration;

use assertables::{assume, assume_eq};
use byteorder::{ByteOrder, LittleEndian};
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};

use dslab_mp::logger::LogEntry;
use dslab_mp::mc::events::EventOrderingMode;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::predicates::{collects, goals, invariants, prunes};
use dslab_mp::mc::state::McState;
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategy::{InvariantFn, McStats, StrategyConfig};
use dslab_mp::mc::system::McSystem;
use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// MESSAGES ----------------------------------------------------------------------------------------

#[derive(Serialize)]
struct GetReqMessage<'a> {
    key: &'a str,
    quorum: u8,
}

#[derive(Deserialize)]
struct GetRespMessage<'a> {
    key: &'a str,
    values: Vec<&'a str>,
    context: Option<Cow<'a, str>>,
}

#[derive(Serialize)]
struct PutReqMessage<'a> {
    key: &'a str,
    value: &'a str,
    context: Option<String>,
    quorum: u8,
}

#[derive(Deserialize)]
struct PutRespMessage<'a> {
    key: &'a str,
    values: Vec<&'a str>,
    context: Cow<'a, str>,
}

// UTILS -------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    proc_factory: &'a PyProcessFactory,
    proc_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    sys.network().set_delays(0.01, 0.1);
    let mut proc_names = Vec::new();
    for n in 0..config.proc_count {
        proc_names.push(format!("{}", n));
    }
    for proc_name in proc_names.iter() {
        let proc = config.proc_factory.build((proc_name, proc_names.clone()), config.seed);
        // process and node on which it runs have the same name
        let node_name = proc_name.clone();
        sys.add_node(&node_name);
        sys.add_process(proc_name, Box::new(proc), &node_name);
        let clock_skew = sys.gen_range(0.0..1.0);
        sys.set_node_clock_skew(&node_name, clock_skew);
    }
    sys
}

fn check_get(
    sys: &mut System,
    proc: &str,
    key: &str,
    quorum: u8,
    expected: Option<Vec<&str>>,
    max_steps: u32,
) -> Result<(Vec<String>, Option<String>), String> {
    sys.send_local_message(proc, Message::json("GET", &GetReqMessage { key, quorum }));
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("GET_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "GET_RESP")?;
    let data: GetRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    if let Some(expected) = expected {
        let mut values_set: HashSet<_> = data.values.clone().into_iter().collect();
        let mut expected_set: HashSet<_> = expected.into_iter().collect();

        if key.starts_with("CART") || key.starts_with("XCART") {
            assert!(values_set.len() <= 1, "Expected no more than 1 value");
            assert!(expected_set.len() <= 1, "Expected cant contain more than 1 value");
            values_set = values_set
                .into_iter()
                .next()
                .map(|s| s.split(',').collect())
                .unwrap_or_default();
            expected_set = expected_set
                .into_iter()
                .next()
                .map(|s| s.split(',').collect())
                .unwrap_or_default();
        }

        assume_eq!(values_set, expected_set)?;
    }
    Ok((
        data.values.iter().map(|x| x.to_string()).collect(),
        data.context.map(|x| x.to_string()),
    ))
}

fn check_put(
    sys: &mut System,
    proc: &str,
    key: &str,
    value: &str,
    context: Option<String>,
    quorum: u8,
    max_steps: u32,
) -> Result<(Vec<String>, String), String> {
    send_put(sys, proc, key, value, quorum, context);
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    Ok((
        data.values.iter().map(|x| x.to_string()).collect(),
        data.context.to_string(),
    ))
}

fn send_put(sys: &mut System, proc: &str, key: &str, value: &str, quorum: u8, context: Option<String>) {
    sys.send_local_message(
        proc,
        Message::json(
            "PUT",
            &PutReqMessage {
                key,
                value,
                quorum,
                context,
            },
        ),
    );
}

fn check_put_result(sys: &mut System, proc: &str, key: &str, max_steps: u32) -> TestResult {
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    Ok(true)
}

fn check_cart_values(values: &[String], expected: &HashSet<&str>) -> TestResult {
    assume_eq!(values.len(), 1, "Expected single value")?;
    let items: Vec<&str> = values[0].split(',').collect();
    assume_eq!(
        items.len(),
        expected.len(),
        format!("Expected {} items in the cart", expected.len())
    )?;
    let items_set: HashSet<&str> = HashSet::from_iter(items);
    assume_eq!(items_set, *expected)
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
    let dist = WeightedIndex::new(WEIGHTS).unwrap();
    rand.sample_iter(&dist).take(length).map(|x| SYMBOLS[x]).collect()
}

fn key_replicas(key: &str, sys: &System) -> Vec<String> {
    let proc_count = sys.process_names().len();
    let mut replicas = Vec::new();
    let hash = md5::compute(key);
    let hash128 = LittleEndian::read_u128(&hash.0);
    let mut replica = (hash128 % proc_count as u128) as usize;
    for _ in 0..3 {
        replicas.push(replica.to_string());
        replica += 1;
        if replica == proc_count {
            replica = 0;
        }
    }
    replicas
}

fn key_non_replicas(key: &str, sys: &System) -> Vec<String> {
    let replicas = key_replicas(key, sys);
    let mut non_replicas_pre = Vec::new();
    let mut non_replicas = Vec::new();
    let mut pre = true;
    let mut process_names = sys.process_names();
    process_names.sort();
    for proc in process_names {
        if replicas.contains(&proc) {
            pre = false;
            continue;
        }
        if pre {
            non_replicas_pre.push(proc);
        } else {
            non_replicas.push(proc);
        }
    }
    non_replicas.append(&mut non_replicas_pre);
    non_replicas
}

// TESTS -------------------------------------------------------------------------------------------

fn test_basic(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);

    // get key from the first node
    check_get(&mut sys, &procs[0], &key, 2, Some(vec![]), 100)?;

    // put key from the first replica
    let value = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, &replicas[0], &key, &value, None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value)?;

    // get key from the last replica
    check_get(&mut sys, &replicas[2], &key, 2, Some(vec![&value]), 100)?;

    // get key from the first non-replica
    check_get(&mut sys, &non_replicas[0], &key, 2, Some(vec![&value]), 100)?;

    // update key from the last non-replica
    let (_, ctx) = check_get(&mut sys, &non_replicas[2], &key, 2, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, &non_replicas[2], &key, &value2, ctx, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value2)?;

    // get key from the first node
    check_get(&mut sys, &procs[0], &key, 2, Some(vec![&value2]), 100)?;
    Ok(true)
}

fn test_stale_replica(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first non-replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[0], &key, &value, None, 3, 100)?;

    // disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the last replica with quorum 2
    let (_, ctx) = check_get(&mut sys, &replicas[2], &key, 2, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[2], &key, &value2, ctx, 2, 100)?;

    // disconnect the last replica
    sys.network().disconnect_node(&replicas[2]);
    // connect the first replica
    sys.network().connect_node(&replicas[0]);

    // read key from the second replica with quorum 2
    check_get(&mut sys, &replicas[1], &key, 2, Some(vec![&value2]), 100)?;

    // step for a while and check whether the first replica got the recent value
    sys.steps(100);
    sys.network().disconnect_node(&replicas[0]);
    check_get(&mut sys, &replicas[0], &key, 1, Some(vec![&value2]), 100)?;
    Ok(true)
}

#[allow(clippy::get_first)]
fn test_concurrent_writes_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas.get(0).unwrap();
    let proc_2 = &non_replicas.get(1).unwrap();
    let proc_3 = &non_replicas.get(2).unwrap();

    // put key from proc_1 (quorum=2)
    let value1 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, proc_1, &key, &value1, None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value1)?;

    // concurrently (using same context) put key from proc_2 (quorum=2)
    let value2 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, proc_2, &key, &value2, None, 2, 100)?;
    assume_eq!(values.len(), 2, "Expected two values")?;

    // read key from proc_3 (quorum=2)
    // should return both values for reconciliation by the client
    check_get(&mut sys, proc_3, &key, 2, Some(vec![&value1, &value2]), 100)?;
    Ok(true)
}

#[allow(clippy::get_first)]
fn test_concurrent_writes_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas.get(0).unwrap();
    let proc_2 = &non_replicas.get(1).unwrap();
    let proc_3 = &non_replicas.get(2).unwrap();

    // put key from proc_1 (quorum=2)
    let value1 = random_string(8, &mut rand);
    send_put(&mut sys, proc_1, &key, &value1, 2, None);

    // concurrently (using same context) put key from proc_2 (quorum=2)
    let value2 = random_string(8, &mut rand);
    send_put(&mut sys, proc_2, &key, &value2, 2, None);

    // wait until both puts are processed
    check_put_result(&mut sys, proc_1, &key, 100)?;
    check_put_result(&mut sys, proc_2, &key, 100)?;

    // read key from proc_3 (quorum=2)
    // should return both values for reconciliation by the client
    let (_, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&value1, &value2]), 100)?;
    // put new reconciled value using the obtained context
    let value3 = [value1, value2].join("+");
    check_put(&mut sys, proc_3, &key, &value3, ctx, 2, 100)?;

    // read key from proc_1 (quorum=2)
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&value3]), 100)?;
    Ok(true)
}

fn test_concurrent_writes_3(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first replica (quorum=1)
    let value1 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value1, None, 1, 100)?;

    // concurrently put key from the second replica (quorum=1)
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[1], &key, &value2, None, 1, 100)?;

    // read key from the first non-replica (quorum=3)
    check_get(&mut sys, &non_replicas[0], &key, 3, Some(vec![&value1, &value2]), 100)?;
    Ok(true)
}

fn test_diverged_replicas(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, None, 3, 100)?;

    // disconnect each replica and put value from it
    let mut new_values = Vec::new();
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
    }
    for replica in replicas.iter() {
        let (_, ctx) = check_get(&mut sys, replica, &key, 1, Some(vec![&value]), 100)?;
        let value2 = random_string(8, &mut rand);
        check_put(&mut sys, replica, &key, &value2, ctx, 1, 100)?;
        new_values.push(value2);
        // read some key to advance the time
        // (make sure that the isolated replicas are not among this key's replicas)
        loop {
            let some_key = random_string(8, &mut rand).to_uppercase();
            let some_key_replicas = key_replicas(&some_key, &sys);
            if replicas.iter().all(|proc| !some_key_replicas.contains(proc)) {
                check_get(&mut sys, &non_replicas[0], &some_key, 3, Some(vec![]), 100)?;
                break;
            }
        }
    }

    // reconnect the replicas
    for replica in replicas.iter() {
        sys.network().connect_node(replica);
    }

    // read key from the first replica with quorum 3
    // should return all three conflicting values
    let expected = new_values.iter().map(String::as_str).collect();
    check_get(&mut sys, &replicas[0], &key, 3, Some(expected), 100)?;
    Ok(true)
}

fn test_sloppy_quorum(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first non-replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[0], &key, &value, None, 3, 100)?;

    // temporarily disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the second non-replica with quorum 3 (should use sloppy quorum)
    let (_, ctx) = check_get(&mut sys, &non_replicas[1], &key, 1, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[1], &key, &value2, ctx, 3, 100)?;

    // read key from the last non-replica with quorum 3 (should use sloppy quorum)
    check_get(&mut sys, &non_replicas[2], &key, 3, Some(vec![&value2]), 100)?;

    // reconnect the first replica and let it receive the update
    sys.network().connect_node(&replicas[0]);
    sys.steps(100);

    // check if the first replica got update
    sys.network().disconnect_node(&replicas[0]);
    check_get(&mut sys, &replicas[0], &key, 1, Some(vec![&value2]), 100)?;
    Ok(true)
}

fn test_partitioned_clients(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let non_replica1 = &non_replicas[0];
    let non_replica2 = &non_replicas[1];
    let non_replica3 = &non_replicas[2];

    // put key from the first node with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &procs[0], &key, &value, None, 3, 100)?;

    // partition network into two parts
    let part1: Vec<&str> = vec![non_replica1, non_replica2, replica1];
    let part2: Vec<&str> = vec![non_replica3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1
    let (values, ctx) = check_get(&mut sys, non_replica1, &key, 2, Some(vec![&value]), 100)?;
    let mut value2 = format!("{}-1", values[0]);
    check_put(&mut sys, non_replica1, &key, &value2, ctx, 2, 100)?;
    let (values, ctx) = check_get(&mut sys, non_replica2, &key, 2, Some(vec![&value2]), 100)?;
    value2 = format!("{}-2", values[0]);
    check_put(&mut sys, non_replica2, &key, &value2, ctx, 2, 100)?;
    check_get(&mut sys, non_replica2, &key, 2, Some(vec![&value2]), 100)?;

    // partition 2
    let (values, ctx) = check_get(&mut sys, non_replica3, &key, 2, Some(vec![&value]), 100)?;
    let value3 = format!("{}-3", values[0]);
    check_put(&mut sys, non_replica3, &key, &value3, ctx, 2, 100)?;
    check_get(&mut sys, non_replica3, &key, 2, Some(vec![&value3]), 100)?;

    // heal partition
    sys.network().reset();
    sys.steps(100);

    // read key from all non-replicas
    // (should return value2 and value3)
    let expected: Option<Vec<&str>> = Some(vec![&value2, &value3]);
    check_get(&mut sys, non_replica1, &key, 2, expected.clone(), 100)?;
    check_get(&mut sys, non_replica2, &key, 2, expected.clone(), 100)?;
    check_get(&mut sys, non_replica3, &key, 2, expected.clone(), 100)?;

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        check_get(&mut sys, replica, &key, 1, expected.clone(), 100)?;
    }
    Ok(true)
}

fn test_shopping_cart_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("cart-{}", random_string(8, &mut rand)).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];

    // proc_1: put [milk]
    let mut cart1 = vec!["milk"];
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: put [eggs]
    let mut cart2 = vec!["eggs"];
    let (values, ctx2) = check_put(&mut sys, proc_2, &key, &cart2.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart2 = values[0].split(',').collect();

    // proc_1: put [flour]
    cart1.push("flour");
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx1), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: put [ham]
    cart2.push("ham");
    let (values, _) = check_put(&mut sys, proc_2, &key, &cart2.join(","), Some(ctx2), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;

    // proc_1: put [flour]
    cart1.push("bacon");
    let (values, _) = check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx1), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;

    // read cart from all non-replicas
    let expected: HashSet<_> = vec!["milk", "eggs", "flour", "ham", "bacon"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_cart_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("cart-{}", random_string(8, &mut rand)).to_uppercase();

    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: put [beer, snacks]
    let cart0 = vec!["beer", "snacks"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition network into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: put [milk]
    let mut cart1 = cart0.clone();
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, put [eggs]
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine]
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("cheese");
    cart3.push("wine");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset();
    sys.steps(100);

    // read key from all non-replica nodes
    let expected: HashSet<_> = vec!["cheese", "wine", "milk", "eggs", "beer", "snacks"]
        .into_iter()
        .collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_xcart_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: [beer, snacks]
    let cart0 = vec!["beer", "snacks"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition network into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: put [milk]
    let mut cart1 = cart0.clone();
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, put [eggs]
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine]
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("cheese");
    cart3.push("wine");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset();
    sys.steps(100);

    // read key from all non-replica nodes
    let expected: HashSet<_> = vec!["cheese", "wine", "milk", "eggs"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_xcart_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: put [lemonade, snacks, beer]
    let cart0 = vec!["lemonade", "snacks", "beer"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition network into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: remove [lemonade], put [milk]
    let mut cart1 = cart0.clone();
    cart1.remove(0);
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, put [eggs]
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine], put [snacks] (back)
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("lemonade");
    cart3.push("cheese");
    cart3.push("wine");
    let (_, ctx) = check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    cart3.push("snacks");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), Some(ctx), 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset();
    sys.steps(100);

    // read key from all non-replica nodes
    let expected: HashSet<_> = vec!["milk", "eggs", "wine", "snacks", "cheese"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

// MODEL CHECKING ------------------------------------------------------------------------------------------------------

fn sorted_cart(cart: &Vec<&str>) -> Vec<String> {
    let mut res = vec![];
    for items in cart {
        let mut items = items.split(',').map(|s| s.to_owned()).collect::<Vec<String>>();
        items.sort();
        res.push(items.join(","));
    }
    res.sort();
    res
}

fn mc_get_invariant<S>(proc: S, key: String, expected: Vec<String>) -> InvariantFn
where
    S: Into<String>,
{
    let proc_name = proc.into();
    Box::new(move |state: &McState| -> Result<(), String> {
        for entry in state.current_run_trace().iter() {
            if let LogEntry::McLocalMessageSent { msg, proc } = entry {
                if &proc_name != proc {
                    return Err("local message received on wrong process".to_string());
                }
                if msg.tip != "GET_RESP" {
                    return Err(format!("wrong type {}", msg.tip));
                }
                let data: GetRespMessage = serde_json::from_str(&msg.data).map_err(|err| err.to_string())?;
                if data.key != key {
                    return Err(format!("wrong key {}", data.key));
                }
                if sorted_cart(&data.values) != sorted_cart(&expected.iter().map(|x| x.as_str()).collect()) {
                    return Err(format!("wrong values {:?}", sorted_cart(&data.values)));
                }
            }
        }
        Ok(())
    })
}

fn mc_put_invariant<S>(proc: S, key: String, values: Vec<String>) -> InvariantFn
where
    S: Into<String>,
{
    let proc_name = proc.into();
    Box::new(move |state: &McState| -> Result<(), String> {
        for entry in state.current_run_trace().iter() {
            if let LogEntry::McLocalMessageSent { msg, proc } = entry {
                if &proc_name != proc {
                    return Err("local message received on wrong process".to_string());
                }
                if msg.tip != "PUT_RESP" {
                    return Err(format!("wrong type {}", msg.tip));
                }
                let data: PutRespMessage = serde_json::from_str(&msg.data).map_err(|err| err.to_string())?;
                if data.key != key {
                    return Err(format!("wrong key {}", data.key));
                }
                if sorted_cart(&data.values) != sorted_cart(&values.iter().map(|x| x.as_str()).collect()) {
                    return Err(format!("wrong values {:?}", sorted_cart(&data.values)));
                }
            }
        }
        Ok(())
    })
}

enum McQuery {
    Get(String, Vec<String>),
    Put(String, Vec<String>),
}

enum McNetworkChange {
    None,
    Reset,
    Partition([Vec<String>; 2]),
    Isolation,
}

fn mc_query_strategy(proc: &str, query_data: McQuery) -> StrategyConfig {
    let proc_name = proc.to_string();

    let invariant = match query_data {
        McQuery::Get(key, expected) => mc_get_invariant(proc, key, expected),
        McQuery::Put(key, value) => mc_put_invariant(proc, key, value),
    };

    StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 5_usize),
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 10_usize),
        ]))
        .goal(goals::event_happened_n_times_current_run(
            LogEntry::is_mc_local_message_sent,
            1,
        ))
        .invariant(invariants::all_invariants(vec![
            invariant,
            invariants::time_limit(Duration::from_secs(120)),
        ]))
        .collect(collects::event_happened_n_times_current_run(
            move |log_entry| match log_entry {
                LogEntry::McLocalMessageSent { proc, .. } => proc == &proc_name,
                _ => false,
            },
            1,
        ))
}

fn run_mc<S>(
    mc: &mut ModelChecker,
    strategy_config: StrategyConfig,
    proc: S,
    msg: Message,
    network_change: McNetworkChange,
    states: Option<HashSet<McState>>,
) -> Result<McStats, String>
where
    S: Into<String>,
{
    let proc = proc.into();

    let callback = |sys: &mut McSystem| {
        match &network_change {
            McNetworkChange::Partition([part1, part2]) => sys.network().partition(part1, part2),
            McNetworkChange::Reset => sys.network().reset(),
            McNetworkChange::Isolation => {
                for node in sys.nodes() {
                    sys.network().disconnect_node(&node)
                }
            }
            McNetworkChange::None => {}
        }
        sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
        sys.send_local_message(proc.clone(), proc.clone(), msg.clone());
    };

    let res = if let Some(states) = states {
        // println!("states {:#?}", states);
        mc.run_from_states_with_change::<Bfs>(strategy_config, states, callback)
    } else {
        mc.run_with_change::<Bfs>(strategy_config, callback)
    };
    match res {
        Err(e) => {
            e.print_trace();
            Err(e.message())
        }
        Ok(stats) => Ok(stats),
    }
}

fn mc_stabilize(sys: &mut System, states: HashSet<McState>) -> Result<McStats, String> {
    let strategy_config = StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 6),
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 24),
        ]))
        .goal(goals::any_goal(vec![goals::depth_reached(30), goals::no_events()]))
        .collect(collects::any_collect(vec![
            collects::no_events(),
            collects::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 6),
            collects::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 24),
        ]));
    let mut mc = ModelChecker::new(sys);
    let res = mc.run_from_states_with_change::<Bfs>(strategy_config, states, |sys| {
        sys.network().reset();
        sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
    });
    match res {
        Err(e) => {
            e.print_trace();
            Err(e.message())
        }
        Ok(stats) => Ok(stats),
    }
}

fn test_mc_basic(config: &TestConfig) -> TestResult {
    let sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let mut mc = ModelChecker::new(&sys);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // stage 1: get key from the first node
    let stage1_strategy = mc_query_strategy(&procs[0], McQuery::Get(key.clone(), vec![]));
    let stage1_msg = Message::json("GET", &GetReqMessage { key: &key, quorum: 2 });
    let stage1_states = run_mc(
        &mut mc,
        stage1_strategy,
        &procs[0],
        stage1_msg,
        McNetworkChange::None,
        None,
    )?
    .collected_states;
    println!("stage 1: {}", stage1_states.len());
    if stage1_states.is_empty() {
        return Err("stage 1 - GET response is not received".to_owned());
    }

    // stage 2: put key to the first replica
    let value = random_string(8, &mut rand);
    mc = ModelChecker::new(&sys);

    let stage2_strategy = mc_query_strategy(&replicas[0], McQuery::Put(key.clone(), vec![value.clone()]));
    let stage2_strategy = stage2_strategy.prune(prunes::any_prune(vec![
        prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 2),
        prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 24),
        prunes::proc_permutations(&replicas),
    ]));
    let stage2_msg = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value,
            quorum: 2,
            context: None,
        },
    );
    let stage2_states = run_mc(
        &mut mc,
        stage2_strategy,
        &replicas[0],
        stage2_msg,
        McNetworkChange::None,
        None,
    )?
    .collected_states;
    println!("stage 2: {}", stage2_states.len());
    if stage2_states.is_empty() {
        return Err("stage 2 - PUT response is not received".to_owned());
    }

    // stage 3: get key from the last replica
    let stage3_strategy = mc_query_strategy(&replicas[2], McQuery::Get(key.clone(), vec![value]));
    let stage3_strategy = stage3_strategy.prune(prunes::any_prune(vec![
        prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 4),
        prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 24),
        // symmetry-breaking constraint: log starts with replica 2 (because it received the query)
        // and then the replica 0 should be earlier in the log than replica 1.
        prunes::proc_permutations(&[replicas[2].clone(), replicas[0].clone(), replicas[1].clone()]),
    ]));
    let stage3_msg = Message::json("GET", &GetReqMessage { key: &key, quorum: 2 });
    let stage3_states = run_mc(
        &mut mc,
        stage3_strategy,
        &replicas[2],
        stage3_msg,
        McNetworkChange::None,
        Some(stage2_states),
    )?
    .collected_states;
    println!("stage 3: {}", stage3_states.len());
    if stage3_states.is_empty() {
        return Err("stage 3 - GET response is not received".to_owned());
    }
    Ok(true)
}

fn test_mc_sloppy_quorum_hinted_handoff(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let mut mc = ModelChecker::new(&sys);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // stage 1: get key from the first replica (during the network partition)
    let stage1_strategy = mc_query_strategy(&replicas[0], McQuery::Get(key.clone(), vec![]));
    let stage1_msg = Message::json("GET", &GetReqMessage { key: &key, quorum: 2 });
    let stage1_states = run_mc(
        &mut mc,
        stage1_strategy,
        &replicas[0],
        stage1_msg,
        McNetworkChange::Partition([
            vec![
                replicas[0].clone(),
                non_replicas[0].clone(),
                non_replicas[1].clone(),
                non_replicas[2].clone(),
            ],
            vec![replicas[1].clone(), replicas[2].clone()],
        ]),
        None,
    )?
    .collected_states;
    println!("stage 1: {}", stage1_states.len());
    if stage1_states.is_empty() {
        return Err("stage 1 - GET response is not received".to_owned());
    }

    // stage 2: put key from the first replica (network partition still exists)
    let value = random_string(8, &mut rand);
    mc = ModelChecker::new(&sys);

    let stage2_strategy = mc_query_strategy(&replicas[0], McQuery::Put(key.clone(), vec![value.clone()]));
    let stage2_msg = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value,
            quorum: 2,
            context: None,
        },
    );
    let stage2_states = run_mc(
        &mut mc,
        stage2_strategy,
        &replicas[0],
        stage2_msg,
        McNetworkChange::Partition([
            vec![
                replicas[0].clone(),
                non_replicas[0].clone(),
                non_replicas[1].clone(),
                non_replicas[2].clone(),
            ],
            vec![replicas[1].clone(), replicas[2].clone()],
        ]),
        None,
    )?
    .collected_states;
    println!("stage 2: {}", stage2_states.len());
    if stage2_states.is_empty() {
        return Err("stage 2 - PUT response is not received".to_owned());
    }

    // stage 3: recover network and let data propagate to all replicas
    let stage3_states = mc_stabilize(&mut sys, stage2_states)?.collected_states;
    println!("stage 3: {}", stage3_states.len());
    if stage3_states.is_empty() {
        return Err("stage 3 - no states found during the exploration phase with recovered network".to_owned());
    }

    // stage 4: get key from the last replica (again during the network partition)
    let stage4_strategy = mc_query_strategy(&replicas[2], McQuery::Get(key.clone(), vec![value]));
    let stage4_msg = Message::json("GET", &GetReqMessage { key: &key, quorum: 2 });
    let stage4_states = run_mc(
        &mut mc,
        stage4_strategy,
        &replicas[2],
        stage4_msg,
        McNetworkChange::Partition([
            vec![
                replicas[0].clone(),
                non_replicas[0].clone(),
                non_replicas[1].clone(),
                non_replicas[2].clone(),
            ],
            vec![replicas[1].clone(), replicas[2].clone()],
        ]),
        Some(stage3_states),
    )?
    .collected_states;
    println!("stage 4: {}", stage4_states.len());
    if stage4_states.is_empty() {
        return Err("stage 4 - GET response is not received".to_owned());
    }
    Ok(true)
}

fn test_mc_concurrent(config: &TestConfig) -> TestResult {
    let sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    let mut mc = ModelChecker::new(&sys);

    // put (key, value) to the first replica
    // and then put (key, value2) to the second replica
    let value = random_string(8, &mut rand);
    let value2 = random_string(8, &mut rand);

    let strategy_config = mc_query_strategy(&replicas[0], McQuery::Put(key.clone(), vec![value.clone()]));
    let msg1 = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value,
            quorum: 1,
            context: None,
        },
    );
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[0],
        msg1,
        McNetworkChange::Isolation,
        None,
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("put({key}, {value}) response is not received"));
    }
    println!("put({key}, {value}): {} states collected", states.len());

    let strategy_config = mc_query_strategy(&replicas[1], McQuery::Put(key.clone(), vec![value2.clone()]));
    let msg2 = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value2,
            quorum: 1,
            context: None,
        },
    );
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[1],
        msg2,
        McNetworkChange::None,
        Some(states),
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("put({key}, {value2}) response is not received"));
    }
    println!("put({key}, {value2}): {} states collected", states.len());

    // now reset the network state and ask the third replica about the key's value
    // we expect both values in the response
    let strategy_config = mc_query_strategy(
        &replicas[2],
        McQuery::Get(key.to_string(), vec![value.to_string(), value2.to_string()]),
    );
    let msg = Message::json("GET", &GetReqMessage { key: &key, quorum: 3 });
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[2],
        msg,
        McNetworkChange::Reset,
        Some(states),
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("get({key}) response is not received"));
    }
    println!("get({key}): {} states collected", states.len());
    Ok(true)
}

fn test_mc_concurrent_cart(config: &TestConfig) -> TestResult {
    let sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("CART_{}", random_string(8, &mut rand).to_uppercase());
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // put key to the first replica
    let value = "a,b,c".to_string();
    let value2 = "b,c,d".to_string();

    let mut mc = ModelChecker::new(&sys);

    let strategy_config = mc_query_strategy(&replicas[0], McQuery::Put(key.clone(), vec![value.clone()]));
    let msg1 = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value,
            quorum: 1,
            context: None,
        },
    );
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[0],
        msg1,
        McNetworkChange::Isolation,
        None,
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("put({key}, \"{value}\") response is not received"));
    }
    println!("put({key}, \"{value}\"): {} states collected", states.len());

    let strategy_config = mc_query_strategy(&replicas[1], McQuery::Put(key.clone(), vec![value2.clone()]));
    let msg2 = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value2,
            quorum: 1,
            context: None,
        },
    );
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[1],
        msg2,
        McNetworkChange::None,
        Some(states),
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("put({key}, \"{value2}\") response is not received"));
    }
    println!("put({key}, \"{value2}\"): {} states collected", states.len());

    // Now reset the network state and ask all replicas to agree about value for key
    // We expect the combination of "a,b,c" and "b,c,d"
    let strategy_config = mc_query_strategy(&replicas[2], McQuery::Get(key.to_string(), vec!["a,b,c,d".to_string()]));
    let msg3 = Message::json("GET", &GetReqMessage { key: &key, quorum: 3 });
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[2],
        msg3,
        McNetworkChange::Reset,
        Some(states),
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("get({key}) has no positive outcomes"));
    }
    println!("get({key}): {} states collected", states.len());
    Ok(true)
}

fn test_mc_concurrent_xcart(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let mut mc = ModelChecker::new(&sys);

    let value1 = "a,b".to_string();
    let value2 = "a,b,c".to_string();
    let value3 = "d".to_string();
    let value4 = "c,d".to_string();

    let strategy_config = mc_query_strategy(&replicas[2], McQuery::Put(key.clone(), vec![value1.clone()]));
    let msg1 = Message::json(
        "PUT",
        &PutReqMessage {
            key: &key,
            value: &value1,
            quorum: 3,
            context: None,
        },
    );
    let states = run_mc(
        &mut mc,
        strategy_config,
        &replicas[2],
        msg1,
        McNetworkChange::None,
        None,
    )?
    .collected_states;
    if states.is_empty() {
        return Err(format!("put({key}, \"{value1}\") response is not received"));
    }
    println!("get({key}): {} states collected", states.len());

    let states = mc_stabilize(&mut sys, states)?.collected_states;
    println!("after stabilization: {} states collected", states.len());

    for state in &states {
        let msg = &state.node_states[&replicas[2]].proc_states[&replicas[2]].local_outbox[0];
        let data: PutRespMessage = serde_json::from_str(&msg.data).map_err(|err| err.to_string())?;
        let ctx = data.context.to_string();

        let msg1 = Message::json(
            "PUT",
            &PutReqMessage {
                key: &key,
                value: &value2,
                quorum: 1,
                context: Some(ctx.clone()),
            },
        );
        let strategy_config = mc_query_strategy(&replicas[0], McQuery::Put(key.clone(), vec![value2.clone()]));
        mc = ModelChecker::new(&sys);
        let cur_states: HashSet<McState> = run_mc(
            &mut mc,
            strategy_config,
            &replicas[0],
            msg1,
            McNetworkChange::Isolation,
            Some(HashSet::from_iter(vec![state.clone()])),
        )?
        .collected_states;
        if states.is_empty() {
            return Err(format!("put({key}, \"{value2}\") response is not received"));
        }

        let msg2 = Message::json(
            "PUT",
            &PutReqMessage {
                key: &key,
                value: &value3,
                quorum: 1,
                context: Some(ctx),
            },
        );
        let strategy_config = mc_query_strategy(&replicas[1], McQuery::Put(key.clone(), vec![value3.clone()]));
        let cur_states = run_mc(
            &mut mc,
            strategy_config,
            &replicas[1],
            msg2,
            McNetworkChange::Isolation,
            Some(cur_states),
        )?
        .collected_states;
        if states.is_empty() {
            return Err(format!("put({key}, \"{value3}\") response is not received"));
        }

        let msg3 = Message::json("GET", &GetReqMessage { key: &key, quorum: 3 });
        let strategy_config = mc_query_strategy(&replicas[2], McQuery::Get(key.clone(), vec![value4.clone()]));
        run_mc(
            &mut mc,
            strategy_config,
            &replicas[2],
            msg3,
            McNetworkChange::Reset,
            Some(cur_states),
        )?;
        if states.is_empty() {
            return Err(format!("get({key}) response is not received"));
        }
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Replicated KV Store v2 Homework Tests
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

    /// Number of processes
    #[clap(long, short, default_value = "6")]
    proc_count: u32,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Do not run model checking tests
    #[clap(long)]
    disable_mc_tests: bool,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());

    let proc_factory = PyProcessFactory::new(&args.solution_path, "StorageNode");
    let config = TestConfig {
        proc_factory: &proc_factory,
        proc_count: args.proc_count,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("BASIC", test_basic, config);
    if !args.disable_mc_tests {
        tests.add("MC BASIC", test_mc_basic, config);
    }
    tests.add("STALE REPLICA", test_stale_replica, config);
    tests.add("CONCURRENT WRITES 1", test_concurrent_writes_1, config);
    tests.add("CONCURRENT WRITES 2", test_concurrent_writes_2, config);
    tests.add("CONCURRENT WRITES 3", test_concurrent_writes_3, config);
    if !args.disable_mc_tests {
        tests.add("MC CONCURRENT", test_mc_concurrent, config);
    }
    tests.add("DIVERGED REPLICAS", test_diverged_replicas, config);
    tests.add("SLOPPY QUORUM", test_sloppy_quorum, config);
    if !args.disable_mc_tests {
        tests.add("MC SLOPPY QUORUM", test_mc_sloppy_quorum_hinted_handoff, config);
    }
    tests.add("PARTITIONED CLIENTS", test_partitioned_clients, config);
    tests.add("SHOPPING CART 1", test_shopping_cart_1, config);
    tests.add("SHOPPING CART 2", test_shopping_cart_2, config);
    if !args.disable_mc_tests {
        tests.add("MC CONCURRENT CART", test_mc_concurrent_cart, config);
    }
    tests.add("SHOPPING XCART 1", test_shopping_xcart_1, config);
    tests.add("SHOPPING XCART 2", test_shopping_xcart_2, config);
    if !args.disable_mc_tests {
        tests.add("MC CONCURRENT XCART", test_mc_concurrent_xcart, config);
    }
    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
