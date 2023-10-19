use std::collections::HashSet;
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use byteorder::{ByteOrder, LittleEndian};
use clap::Parser;
use dslab_mp::mc::events::EventOrderingMode;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};

use dslab_mp::logger::LogEntry;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::predicates::{collects, goals, prunes};
use dslab_mp::mc::state::McState;
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategy::{InvariantFn, McStats, StrategyConfig};
use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// MESSAGES ------------------------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct GetReqMessage<'a> {
    key: &'a str,
    quorum: u8,
}

#[derive(Deserialize)]
struct GetRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Serialize)]
struct PutReqMessage<'a> {
    key: &'a str,
    value: &'a str,
    quorum: u8,
}

#[derive(Deserialize)]
struct PutRespMessage<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
struct DeleteReqMessage<'a> {
    key: &'a str,
    quorum: u8,
}

#[derive(Deserialize)]
struct DeleteRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

// UTILS ---------------------------------------------------------------------------------------------------------------

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
    }
    sys
}

fn check_get(
    sys: &mut System,
    proc: &str,
    key: &str,
    quorum: u8,
    expected: Option<&str>,
    max_steps: u32,
) -> TestResult {
    sys.send_local_message(proc, Message::json("GET", &GetReqMessage { key, quorum }));
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

fn send_put(sys: &mut System, proc: &str, key: &str, value: &str, quorum: u8) {
    sys.send_local_message(proc, Message::json("PUT", &PutReqMessage { key, value, quorum }));
}

fn check_put_result(sys: &mut System, proc: &str, key: &str, value: &str, max_steps: u32) -> TestResult {
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

fn check_put(sys: &mut System, proc: &str, key: &str, value: &str, quorum: u8, max_steps: u32) -> TestResult {
    send_put(sys, proc, key, value, quorum);
    check_put_result(sys, proc, key, value, max_steps)
}

fn check_delete(
    sys: &mut System,
    proc: &str,
    key: &str,
    quorum: u8,
    expected: Option<&str>,
    max_steps: u32,
) -> TestResult {
    sys.send_local_message(proc, Message::json("DELETE", &DeleteReqMessage { key, quorum }));
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

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_basic(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // get key from the first node
    check_get(&mut sys, &procs[0], &key, 2, None, 100)?;

    // put key from the first replica
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, 2, 100)?;

    // get key from the last replica
    check_get(&mut sys, &replicas[2], &key, 2, Some(&value), 100)?;

    // get key from the first non-replica
    check_get(&mut sys, &non_replicas[0], &key, 2, Some(&value), 100)?;

    // update key from the last non-replica
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[2], &key, &value2, 2, 100)?;

    // get key from the first node
    check_get(&mut sys, &procs[0], &key, 2, Some(&value2), 100)?;

    // delete key from the second non-replica
    check_delete(&mut sys, &non_replicas[1], &key, 2, Some(&value2), 100)?;

    // get key from the last replica
    check_get(&mut sys, &replicas[2], &key, 2, None, 100)?;

    // get key from the first non-replica
    check_get(&mut sys, &non_replicas[0], &key, 2, None, 100)
}

fn test_replicas_check(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, 3, 100)?;

    // disconnect each replica and check the stored value
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        check_get(&mut sys, replica, &key, 1, Some(&value), 100)?;
    }
    Ok(true)
}

fn test_concurrent_writes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);

    // concurrently put different values from the first and second non-replicas
    let value = random_string(8, &mut rand);
    send_put(&mut sys, &non_replicas[0], &key, &value, 2);
    // small delay to ensure writes will have different times
    sys.step_for_duration(0.01);

    let value2 = random_string(8, &mut rand);
    send_put(&mut sys, &non_replicas[1], &key, &value2, 2);

    // the won value is the one written later
    // but it was not observed by the put from the first replica!
    check_put_result(&mut sys, &non_replicas[0], &key, &value, 100)?;
    check_put_result(&mut sys, &non_replicas[1], &key, &value2, 100)?;

    // get key from the third non-replica with quorum 3
    check_get(&mut sys, &non_replicas[2], &key, 3, Some(&value2), 100)
}

fn test_concurrent_writes_tie(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);

    // concurrently put different values from the first and second non-replicas
    let value = random_string(8, &mut rand);
    send_put(&mut sys, &non_replicas[0], &key, &value, 2);

    let value2 = random_string(8, &mut rand);
    send_put(&mut sys, &non_replicas[1], &key, &value2, 2);

    // with default seed, the won value is from the second replica
    // and is observed by the put from the first replica!
    let won_value = &value2.max(value);
    check_put_result(&mut sys, &non_replicas[0], &key, won_value, 100)?;
    check_put_result(&mut sys, &non_replicas[1], &key, won_value, 100)?;

    // get key from the third non-replica with quorum 3
    check_get(&mut sys, &non_replicas[2], &key, 3, Some(won_value), 100)
}

fn test_stale_replica(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, 3, 100)?;

    // disconnect the last replica
    sys.network().disconnect_node(&replicas[2]);

    // update key from the first replica with quorum 2
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value2, 2, 100)?;

    // disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);
    // connect the last replica
    sys.network().connect_node(&replicas[2]);

    // read key from the second replica with quorum 2
    // should update the last replica via read repair or anti-entropy
    check_get(&mut sys, &replicas[1], &key, 2, Some(&value2), 100)?;

    // step for a while and check whether the last replica got the recent value
    sys.steps(100);
    sys.network().disconnect_node(&replicas[2]);
    check_get(&mut sys, &replicas[2], &key, 1, Some(&value2), 100)
}

fn test_stale_replica_delete(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, 3, 100)?;

    // disconnect the last replica
    sys.network().disconnect_node(&replicas[2]);

    // update key from the first replica with quorum 2
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value2, 2, 100)?;

    // disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);
    // connect the last replica
    sys.network().connect_node(&replicas[2]);

    // delete key from the last replica (should return the last-written value)
    check_delete(&mut sys, &replicas[2], &key, 2, Some(&value2), 100)?;

    // connect the first replica
    sys.network().connect_node(&replicas[0]);

    // get key from the first replica (should return None)
    check_get(&mut sys, &replicas[0], &key, 2, None, 100)
}

fn test_diverged_replicas(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, 3, 100)?;

    // disconnect each replica and update key from it with quorum 1
    let mut new_values = Vec::new();
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let value2 = random_string(8, &mut rand);
        check_put(&mut sys, replica, &key, &value2, 1, 100)?;
        new_values.push(value2);
        // read some key to advance the time
        // (make sure that the isolated replica is not among this key's replicas)
        loop {
            let some_key = random_string(8, &mut rand).to_uppercase();
            if !key_replicas(&some_key, &sys).contains(replica) {
                check_get(&mut sys, &non_replicas[0], &some_key, 3, None, 100)?;
                break;
            }
        }
        sys.network().connect_node(replica);
    }

    // read key from the first replica with quorum 3
    // (the last written value should win)
    let expected = new_values.last().unwrap();
    check_get(&mut sys, &replicas[0], &key, 3, Some(expected), 100)
}

fn test_sloppy_quorum_read(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // put key from the second replica with quorum 2
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[1], &key, &value, 2, 100)?;

    // disconnect the second replica
    sys.network().disconnect_node(&replicas[1]);

    // read key from the last non-replica with quorum 2 (should use sloppy quorum)
    // since non-replicas do not store any value, the last replica's value should win
    // the reading node could also do read repair on non-replicas to fix them
    check_get(&mut sys, &non_replicas[2], &key, 2, Some(&value), 100)
}

fn test_sloppy_quorum_write(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);

    // put key from the first node with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &procs[0], &key, &value, 3, 100)?;

    // temporarily disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the second replica with quorum 3 (should use sloppy quorum)
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[1], &key, &value2, 3, 100)?;

    // read key from the last replica with quorum 3 (should use sloppy quorum)
    check_get(&mut sys, &replicas[2], &key, 3, Some(&value2), 100)?;

    // reconnect the first replica and let it receive the update
    sys.network().connect_node(&replicas[0]);
    sys.steps(100);

    // check if the first replica got update
    sys.network().disconnect_node(&replicas[0]);
    check_get(&mut sys, &replicas[0], &key, 1, Some(&value2), 100)
}

fn test_sloppy_quorum_tricky(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first node with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &procs[0], &key, &value, 3, 100)?;

    // temporarily disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the second replica with quorum 3 (should use sloppy quorum)
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[1], &key, &value2, 3, 100)?;

    // disconnect all members of the previous sloppy quorum
    sys.network().disconnect_node(&replicas[1]);
    sys.network().disconnect_node(&replicas[2]);
    sys.network().disconnect_node(&non_replicas[0]);

    // reconnect the first replica
    sys.network().connect_node(&replicas[0]);

    // now we have only one node storing the key value:
    // - second replica: value (outdated)
    // all connected non-replicas do not store the key

    // read key from the last non-replica with quorum 2
    // (will receive old value only from the first replica and probably read repair it)
    check_get(&mut sys, &non_replicas[2], &key, 2, Some(&value), 100)?;

    // reconnect the second replica
    sys.network().connect_node(&replicas[1]);

    // read key from the last non-replica with quorum 2
    // (should try to contact the main replicas first and receive the new value)
    check_get(&mut sys, &non_replicas[2], &key, 2, Some(&value2), 100)
}

fn test_partition_clients(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // partition clients from all replicas
    let client1 = &non_replicas[0];
    let client2 = &non_replicas[1];
    let part1: Vec<&str> = replicas.iter().map(|s| &**s).collect();
    let part2: Vec<&str> = non_replicas.iter().map(|s| &**s).collect();
    sys.network().make_partition(&part1, &part2);

    // put key from client1 with quorum 2 (should use sloppy quorum without any normal replica)
    let value = random_string(8, &mut rand);
    check_put(&mut sys, client1, &key, &value, 2, 100)?;

    // read key from client2 with quorum 2 (should use sloppy quorum without any normal replica)
    check_get(&mut sys, client2, &key, 2, Some(&value), 100)
}

fn test_partition_mixed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let nodes = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let client1 = &non_replicas[0];
    let client2 = &non_replicas[1];
    let client3 = &non_replicas[2];

    // put key from the first node with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &nodes[0], &key, &value, 3, 100)?;

    // partition clients and replicas
    let part1: Vec<&str> = vec![client1, client2, replica1];
    let part2: Vec<&str> = vec![client3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1
    check_get(&mut sys, client1, &key, 2, Some(&value), 100)?;
    let mut value2 = format!("{}-1", value);
    check_put(&mut sys, client1, &key, &value2, 2, 100)?;
    check_get(&mut sys, client2, &key, 2, Some(&value2), 100)?;
    value2 = format!("{}-2", value2);
    check_put(&mut sys, client2, &key, &value2, 2, 100)?;
    check_get(&mut sys, client2, &key, 2, Some(&value2), 100)?;

    // partition 2
    check_get(&mut sys, client3, &key, 2, Some(&value), 100)?;
    let value3 = format!("{}-3", value);
    check_put(&mut sys, client3, &key, &value3, 2, 100)?;
    check_get(&mut sys, client3, &key, 2, Some(&value3), 100)?;

    // heal partition
    sys.network().reset();
    sys.steps(100);

    // read key from all clients (should return the last-written value)
    check_get(&mut sys, client1, &key, 2, Some(&value3), 100)?;
    check_get(&mut sys, client2, &key, 2, Some(&value3), 100)?;
    check_get(&mut sys, client3, &key, 2, Some(&value3), 100)?;

    // check all replicas (should return the last-written value)
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        check_get(&mut sys, replica, &key, 1, Some(&value3), 100)?;
    }
    Ok(true)
}

fn mc_get_invariant<S>(node: S, proc: S, key: S, expected: Option<S>) -> InvariantFn
where
    S: Into<String>,
{
    let node = node.into();
    let proc = proc.into();
    let key = key.into();
    let expected = expected.map(|x| x.into());
    Box::new(move |state: &McState| -> Result<(), String> {
        let messages = &state.node_states[&node].proc_states[&proc].local_outbox;
        if let Some(message) = messages.get(0) {
            if message.tip != "GET_RESP" {
                return Err(format!("wrong type {}", message.tip));
            }
            let data: GetRespMessage = serde_json::from_str(&message.data).map_err(|err| err.to_string())?;
            if data.key != key {
                return Err(format!("wrong key {}", data.key));
            }
            if data.value.map(|x| x.to_string()) != expected {
                return Err(format!("wrong value {:?}", data.value));
            }
        }
        Ok(())
    })
}

fn mc_put_invariant<S>(node: S, proc: S, key: S, value: S) -> InvariantFn
where
    S: Into<String>,
{
    let node = node.into();
    let proc = proc.into();
    let key = key.into();
    let value = value.into();
    Box::new(move |state: &McState| -> Result<(), String> {
        let messages = &state.node_states[&node].proc_states[&proc].local_outbox;
        if let Some(message) = messages.get(0) {
            if message.tip != "PUT_RESP" {
                return Err(format!("wrong type {}", message.tip));
            }
            let data: PutRespMessage = serde_json::from_str(&message.data).map_err(|err| err.to_string())?;
            if data.key != key {
                return Err(format!("wrong key {}", data.key));
            }
            if data.value != value {
                return Err(format!("wrong value {:?}", data.value));
            }
        }
        Ok(())
    })
}

fn mc_check_query(
    sys: &mut System,
    node: &str,
    proc: &str,
    invariant: InvariantFn,
    msg: Message,
    start_states: Option<HashSet<McState>>,
) -> Result<McStats, String> {
    let proc_name = proc.to_string();
    let strategy_config = StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 5_usize),
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, 10_usize),
        ]))
        .goal(goals::got_n_local_messages(node, proc, 1))
        .invariant(invariant)
        .collect(collects::event_happened_n_times_current_run(
            move |log_entry| match log_entry {
                LogEntry::McLocalMessageSent { proc, .. } => proc == &proc_name,
                _ => false,
            },
            1,
        ));
    let mut mc = ModelChecker::new(sys);

    let res = if let Some(start_states) = start_states {
        mc.run_from_states_with_change::<Bfs>(strategy_config, start_states, |sys| {
            sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
            sys.send_local_message(node, proc, msg.clone());
        })
    } else {
        mc.run_with_change::<Bfs>(strategy_config, |sys| {
            sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
            sys.send_local_message(node, proc, msg)
        })
    };
    match res {
        Err(e) => {
            e.print_trace();
            Err(e.message())
        }
        Ok(stats) => Ok(stats),
    }
}

fn check_mc_get(
    sys: &mut System,
    node: &str,
    proc: &str,
    key: &str,
    expected: Option<&str>,
    quorum: u8,
    start_states: Option<HashSet<McState>>,
) -> Result<McStats, String> {
    println!("check_mc_get");
    let msg = Message::new("GET", &format!(r#"{{"key": "{}", "quorum": {}}}"#, key, quorum));
    mc_check_query(
        sys,
        node,
        proc,
        mc_get_invariant(node, proc, key, expected),
        msg,
        start_states,
    )
}

fn check_mc_put(
    sys: &mut System,
    node: &str,
    proc: &str,
    key: &str,
    value: &str,
    quorum: u8,
    start_states: Option<HashSet<McState>>,
) -> Result<McStats, String> {
    println!("check_mc_put");
    let msg = Message::new(
        "PUT",
        &format!(r#"{{"key": "{}", "value": "{}", "quorum": {}}}"#, key, value, quorum),
    );
    mc_check_query(
        sys,
        node,
        proc,
        mc_put_invariant(node, proc, key, value),
        msg,
        start_states,
    )
}

fn mc_stabilize(sys: &mut System, num_steps: u64, start_states: Option<HashSet<McState>>) -> Result<McStats, String> {
    let strategy_config = StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 6),
            prunes::event_happened_n_times_current_run(LogEntry::is_mc_message_received, (num_steps + 1) as usize),
        ]))
        .goal(goals::any_goal(vec![
            goals::depth_reached(num_steps),
            goals::no_events(),
        ]))
        .collect(collects::any_collect(vec![
            collects::no_events(),
            collects::event_happened_n_times_current_run(LogEntry::is_mc_timer_fired, 6),
            collects::event_happened_n_times_current_run(LogEntry::is_mc_message_received, (num_steps + 1) as usize),
        ]));
    let mut mc = ModelChecker::new(sys);
    let res = if let Some(start_states) = start_states {
        mc.run_from_states_with_change::<Bfs>(strategy_config, start_states, |sys| {
            sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
            sys.network().reset();
        })
    } else {
        mc.run_with_change::<Bfs>(strategy_config, |sys| {
            sys.network().reset();
            sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
        })
    };
    match res {
        Err(e) => {
            e.print_trace();
            Err(e.message())
        }
        Ok(stats) => Ok(stats),
    }
}

fn test_mc_basic(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let mut procs = sys.process_names();
    procs.sort();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(key, &sys);
    let non_replicas = key_non_replicas(key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    let mut start_states: HashSet<McState>;
    // get key from the first node
    start_states = check_mc_get(&mut sys, &procs[0], &procs[0], key, None, 2, None)?.collected_states;
    println!("stage 1: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 1 has no positive outcomes".to_owned());
    }

    // put key from the first replica
    let value = "9ps2p1ua";
    start_states = check_mc_put(&mut sys, &replicas[0], &replicas[0], key, value, 2, None)?.collected_states;
    println!("stage 2: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }

    start_states = mc_stabilize(&mut sys, 15, Some(start_states))?.collected_states;
    println!("stage 3: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 3 has no positive outcomes".to_owned());
    }

    // get key from the last replica
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        key,
        Some(value),
        2,
        Some(start_states),
    )?
    .collected_states;
    println!("stage 4: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 4 has no positive outcomes".to_owned());
    }
    Ok(true)
}

fn test_mc_sloppy_quorum_hinted_handoff(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let mut procs = sys.process_names();
    procs.sort();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(key, &sys);
    let non_replicas = key_non_replicas(key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    sys.network()
        .make_partition(&[&replicas[0], &non_replicas[0]], &[&replicas[1], &replicas[2]]);

    let mut start_states: HashSet<McState>;
    // get key from the first node
    start_states = check_mc_get(&mut sys, &replicas[0], &replicas[0], key, None, 2, None)?.collected_states;
    println!("stage 1: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 1 has no positive outcomes".to_owned());
    }
    // put key from the first replica
    let value = "9ps2p1ua";
    start_states = check_mc_put(&mut sys, &replicas[0], &replicas[0], key, value, 2, None)?.collected_states;
    println!("stage 2: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }

    sys.network().reset();

    start_states = mc_stabilize(&mut sys, 30, Some(start_states))?.collected_states;
    println!("stage 3: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 3 has no positive outcomes".to_owned());
    }

    sys.network()
        .make_partition(&[&replicas[0], &non_replicas[0]], &[&replicas[1], &replicas[2]]);
    start_states = start_states
        .into_iter()
        .map(|mut state| {
            state.network.disable_link(&replicas[0], &replicas[1]);
            state.network.disable_link(&replicas[0], &replicas[2]);
            state.network.disable_link(&non_replicas[0], &replicas[1]);
            state.network.disable_link(&non_replicas[0], &replicas[2]);
            state
        })
        .collect();
    // get key from the last replica
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        key,
        Some(value),
        2,
        Some(start_states),
    )?
    .collected_states;
    println!("stage 4: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 4 has no positive outcomes".to_owned());
    }
    Ok(true)
}

fn test_mc_concurrent(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let mut procs = sys.process_names();
    procs.sort();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(key, &sys);
    let non_replicas = key_non_replicas(key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // put key to the first replica
    let value = "9ps2p1ua";
    let value2 = "8ab54uye";

    sys.network().disconnect_node(&replicas[0]);
    sys.network().disconnect_node(&replicas[1]);

    let strategy_config = StrategyConfig::default()
        .goal(goals::got_n_local_messages(&replicas[0], &replicas[0], 1))
        .collect(collects::got_n_local_messages(&replicas[0], &replicas[0], 1));

    let mut mc = ModelChecker::new(&sys);
    let res = mc.run_with_change::<Bfs>(strategy_config, |sys| {
        sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
        sys.send_local_message(
            replicas[0].clone(),
            replicas[0].clone(),
            Message::json("PUT", &PutReqMessage { quorum: 1, key, value }),
        )
    });
    if let Err(e) = res {
        e.print_trace();
        return Err(e.message());
    }
    let start_states = res.unwrap().collected_states;
    println!("stage 1: {}", start_states.len());
    let strategy_config = StrategyConfig::default()
        .goal(goals::got_n_local_messages(&replicas[1], &replicas[1], 1))
        .collect(collects::got_n_local_messages(&replicas[1], &replicas[1], 1));

    let res = mc.run_from_states_with_change::<Bfs>(strategy_config, start_states, |sys| {
        sys.set_event_ordering_mode(EventOrderingMode::MessagesFirst);
        sys.send_local_message(
            replicas[1].clone(),
            replicas[1].clone(),
            Message::json(
                "PUT",
                &PutReqMessage {
                    quorum: 1,
                    key,
                    value: value2,
                },
            ),
        )
    });
    if let Err(e) = res {
        e.print_trace();
        return Err(e.message());
    }

    let mut start_states = res.unwrap().collected_states;
    start_states = start_states
        .into_iter()
        .map(|mut state| {
            state.network.reset();
            state
        })
        .collect();
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        key,
        Some(value2),
        3,
        Some(start_states),
    )?
    .collected_states;
    println!("stage 2: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Replicated KV Store Homework Tests
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
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Debug);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());

    let proc_factory = PyProcessFactory::new(&args.solution_path, "StorageNode");
    let mut config = TestConfig {
        proc_factory: &proc_factory,
        proc_count: args.proc_count,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("BASIC", test_basic, config);
    tests.add("REPLICAS CHECK", test_replicas_check, config);
    tests.add("CONCURRENT WRITES", test_concurrent_writes, config);
    tests.add("CONCURRENT WRITES TIE", test_concurrent_writes_tie, config);
    tests.add("STALE REPLICA", test_stale_replica, config);
    tests.add("STALE REPLICA DELETE", test_stale_replica_delete, config);
    tests.add("DIVERGED REPLICAS", test_diverged_replicas, config);
    tests.add("SLOPPY QUORUM READ", test_sloppy_quorum_read, config);
    tests.add("SLOPPY QUORUM WRITE", test_sloppy_quorum_write, config);
    tests.add("SLOPPY QUORUM TRICKY", test_sloppy_quorum_tricky, config);
    tests.add("PARTITION CLIENTS", test_partition_clients, config);
    tests.add("PARTITION MIXED", test_partition_mixed, config);

    config.proc_count = 4;
    tests.add("MC BASIC", test_mc_basic, config);
    tests.add(
        "MC SLOPPY_QUORUM HINTED_HANDOFF",
        test_mc_sloppy_quorum_hinted_handoff,
        config,
    );
    tests.add("MC CONCURRENT", test_mc_concurrent, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
