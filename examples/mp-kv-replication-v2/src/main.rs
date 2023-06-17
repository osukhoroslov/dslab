use std::borrow::Cow;
use std::collections::HashSet;
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use byteorder::{ByteOrder, LittleEndian};
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// MESSAGES ----------------------------------------------------------------------------------------

#[derive(Serialize)]
struct GetMessage<'a> {
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
struct PutMessage<'a> {
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
    process_factory: &'a PyProcessFactory,
    process_count: u32,
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
    let mut process_names = Vec::new();
    for n in 0..config.process_count {
        process_names.push(format!("{}", n));
    }
    for process_name in process_names.iter() {
        let process = config
            .process_factory
            .build((process_name, process_names.clone()), config.seed);
        let node_name = process_name;
        sys.add_node(node_name);
        sys.add_process(process_name, Box::new(process), process_name);
        let clock_skew = sys.gen_range(0.0..1.0);
        sys.set_node_clock_skew(node_name, clock_skew);
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
    sys.send_local_message(proc, Message::json("GET", &GetMessage { key, quorum }));
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
    sys.send_local_message(
        proc,
        Message::json(
            "PUT",
            &PutMessage {
                key,
                value,
                quorum,
                context,
            },
        ),
    );
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
            &PutMessage {
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

fn check_cart_values(values: &Vec<String>, expected: &HashSet<&str>) -> TestResult {
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
    let process_count = sys.process_names().len();
    let mut replicas = Vec::new();
    let hash = md5::compute(key);
    let hash128 = LittleEndian::read_u128(&hash.0);
    let mut replica = (hash128 % process_count as u128) as usize;
    for _ in 0..3 {
        replicas.push(replica.to_string());
        replica += 1;
        if replica == process_count {
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
    for proc in sys.process_names_sorted() {
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

    // get key from the first process
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

    // get key from the first process
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

    // put key from the first process with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &procs[0], &key, &value, None, 3, 100)?;

    // partition processes into two parts
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
    sys.network().reset_network();
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

    // proc_1: + milk
    let mut cart1 = vec!["milk"];
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: + eggs
    let mut cart2 = vec!["eggs"];
    let (values, ctx2) = check_put(&mut sys, proc_2, &key, &cart2.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart2 = values[0].split(',').collect();

    // proc_1: + flour
    cart1.push("flour");
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx1), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: + ham
    cart2.push("ham");
    let (values, _) = check_put(&mut sys, proc_2, &key, &cart2.join(","), Some(ctx2), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;

    // proc_1: + flour
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

    // proc_1: [beer, snacks]
    let cart0 = vec!["beer", "snacks"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition processes into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: + milk
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
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
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

    // partition processes into two parts
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
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
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

    // partition processes into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: remove lemonade, put milk
    let mut cart1 = cart0.clone();
    cart1.remove(0);
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, + eggs
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine], put snacks (back)
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
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
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

// CLI -----------------------------------------------------------------------------------------------------------------

/// Replicated KV Store v2 Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "../python/solution.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Number of processes used in tests
    #[clap(long, short, default_value = "6")]
    process_count: u32,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let process_factory = PyProcessFactory::new(&args.solution_path, "StorageProcess");
    let config = TestConfig {
        process_factory: &process_factory,
        process_count: args.process_count,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("BASIC", test_basic, config);
    tests.add("STALE REPLICA", test_stale_replica, config);
    tests.add("CONCURRENT WRITES 1", test_concurrent_writes_1, config);
    tests.add("CONCURRENT WRITES 2", test_concurrent_writes_2, config);
    tests.add("CONCURRENT WRITES 3", test_concurrent_writes_3, config);
    tests.add("DIVERGED REPLICAS", test_diverged_replicas, config);
    tests.add("SLOPPY QUORUM", test_sloppy_quorum, config);
    tests.add("PARTITIONED CLIENTS", test_partitioned_clients, config);
    tests.add("SHOPPING CART 1", test_shopping_cart_1, config);
    tests.add("SHOPPING CART 2", test_shopping_cart_2, config);
    tests.add("SHOPPING XCART 1", test_shopping_xcart_1, config);
    tests.add("SHOPPING XCART 2", test_shopping_xcart_2, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
