use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// UTILS -------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct JoinMessage<'a> {
    seed: &'a str,
}

#[derive(Serialize)]
struct LeaveMessage {}

#[derive(Serialize)]
struct GetMembersMessage {}

#[derive(Deserialize)]
struct MembersMessage {
    members: Vec<String>,
}

#[derive(Clone)]
struct TestConfig<'a> {
    process_factory: &'a PyProcessFactory,
    process_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(Some("dslab_mp"), level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    sys.network().set_delays(0.01, 0.1);
    for n in 0..config.process_count {
        // process and node on which it runs have the same name
        let name = format!("{}", &n);
        sys.add_node(&name);
        let clock_skew = sys.gen_range(0.0..10.0);
        sys.set_node_clock_skew(&name, clock_skew);
        let process = config.process_factory.build((&name,), config.seed);
        sys.add_process(&name, boxed!(process), &name);
    }
    sys
}

fn initialize_group(sys: &mut System, group: &Vec<String>, seed: &str) -> TestResult {
    for proc in group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }
    step_until_stabilized(sys, group.clone().into_iter().collect())
}

fn crash_process(name: &str, sys: &mut System) {
    // we just crash the node on which the process is running
    sys.crash_node(name);
}

fn recover_process(name: &str, sys: &mut System, config: &TestConfig) {
    sys.recover_node(name);
    let process = config.process_factory.build((name,), config.seed);
    sys.add_process(name, boxed!(process), name);
}

fn step_until_stabilized(sys: &mut System, group: HashSet<String>) -> TestResult {
    let max_time = sys.time() + 300.; // timeout is 5 minutes
    let mut stabilized = HashSet::new();
    let mut memberlists = HashMap::new();

    while stabilized.len() < group.len() && sys.time() < max_time {
        let cont = sys.step_for_duration(5.);
        stabilized.clear();
        for proc in group.iter() {
            sys.send_local_message(proc, Message::json("GET_MEMBERS", &GetMembersMessage {}));
            let res = sys.step_until_local_message_timeout(proc, 10.);
            assume!(res.is_ok(), format!("Members list is not returned by {}", &proc))?;
            let msgs = res.unwrap();
            let msg = msgs.first().unwrap();
            assume!(msg.tip == "MEMBERS", "Wrong message type")?;
            let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
            let members: HashSet<String> = data.members.clone().into_iter().collect();
            if members.eq(&group) {
                stabilized.insert(proc.clone());
            }
            memberlists.insert(proc.clone(), data.members);
        }
        if !cont {
            break;
        }
    }

    if stabilized != group && group.len() <= 10 {
        println!("Members lists:");
        for proc in sys.process_names() {
            if group.contains(&proc) {
                let members = memberlists.get_mut(&proc).unwrap();
                members.sort();
                println!("- [{}] {}", proc, members.join(", "));
            }
        }
        let mut expected = group.clone().into_iter().collect::<Vec<_>>();
        expected.sort();
        println!("Expected group: {}", expected.join(", "));
    }
    assume_eq!(stabilized, group, "Group members lists are not stabilized")?;
    Ok(true)
}

// TESTS -------------------------------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let group = sys.process_names();
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)
}

fn test_random_seed(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = Vec::new();
    for proc in sys.process_names() {
        let seed = match group.len() {
            0 => &proc,
            _ => group.choose(&mut rand).unwrap(),
        };
        sys.send_local_message(&proc, Message::json("JOIN", &JoinMessage { seed }));
        group.push(proc);
    }
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_join(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let new_proc = group.remove(rand.gen_range(0..group.len()));
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process joins the system
    sys.send_local_message(&new_proc, Message::json("JOIN", &JoinMessage { seed }));
    group.push(new_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_leave(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process leaves the system
    let left_proc = group.remove(rand.gen_range(0..group.len()));
    sys.send_local_message(&left_proc, Message::json("LEAVE", &LeaveMessage {}));
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process crashes
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_process_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // seed process crashes
    group.remove(0);
    crash_process(seed, &mut sys);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_crash_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // process crashes
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // process recovers
    recover_process(&crashed, &mut sys, config);
    sys.send_local_message(&crashed, Message::json("JOIN", &JoinMessage { seed }));

    group.push(crashed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes offline
    let offline_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().disconnect_node(&offline_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_process_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // seed process goes offline
    group.remove(0);
    sys.network().disconnect_node(seed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_offline_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes offline
    let offline_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().disconnect_node(&offline_proc);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // process goes back online
    sys.network().connect_node(&offline_proc);
    group.push(offline_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_network_partition(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group.iter().map(|s| &**s).partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.network().make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())
}

fn test_network_partition_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group.iter().map(|s| &**s).partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.network().make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())?;

    // network is recovered
    sys.network().reset_network();
    step_until_stabilized(&mut sys, group.into_iter().map(String::from).collect())
}

fn test_process_cannot_receive(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes partially offline (cannot receive incoming messages)
    let blocked_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().drop_incoming(&blocked_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_cannot_send(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes partially offline (cannot send outgoing messages)
    let blocked_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().drop_outgoing(&blocked_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_two_processes_cannot_communicate(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // two processes cannot communicate with each other
    let proc1 = seed;
    let proc2 = group.get(rand.gen_range(1..group.len())).unwrap();
    sys.network().disable_link(proc1, proc2);
    sys.network().disable_link(proc2, proc1);
    // run for a while
    sys.steps(1000);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_slow_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // slow down network for a while
    sys.network().set_delays(0.1, 1.0);
    sys.steps(200);
    sys.network().set_delays(0.01, 0.1);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // make network unreliable for a while
    sys.network().set_drop_rate(0.5);
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_on_start(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];

    // make network unreliable from the start
    sys.network().set_drop_rate(0.2);
    for proc in &group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_and_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // make network unreliable for a while + crash process
    sys.network().set_drop_rate(0.5);
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    for _ in 0..5 {
        let p = rand.gen_range(0.0..1.0);
        // do some nasty things
        match p {
            p if p < 0.25 => {
                // crash process
                let victim = group.remove(rand.gen_range(0..group.len()));
                crash_process(&victim, &mut sys);
            }
            p if p < 0.5 => {
                // disconnect process
                let victim = group.remove(rand.gen_range(0..group.len()));
                sys.network().disconnect_node(&victim);
            }
            p if p < 0.75 => {
                // partially disconnect process (cannot receive)
                let victim = group.remove(rand.gen_range(0..group.len()));
                sys.network().drop_incoming(&victim);
            }
            _ => {
                // two processes cannot communicate with each other
                let proc1 = group.get(rand.gen_range(0..group.len())).unwrap();
                let mut proc2 = group.get(rand.gen_range(0..group.len())).unwrap();
                while proc1 == proc2 {
                    proc2 = group.get(rand.gen_range(0..group.len())).unwrap();
                }
                sys.network().disable_link(proc1, proc2);
                sys.network().disable_link(proc2, proc1);
            }
        }
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;
    }
    Ok(true)
}

fn test_scalability_normal(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.process_count,
        config.process_count * 2,
        config.process_count * 5,
        config.process_count * 10,
    ];
    let mut measurements = Vec::new();
    for size in sys_sizes {
        let mut run_config = config.clone();
        run_config.process_count = size;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.process_names();
        group.shuffle(&mut rand);
        let seed = &group[0];
        initialize_group(&mut sys, &group, seed)?;

        let init_time = sys.time();
        let init_net_traffic = sys.network().traffic();
        let init_msg_count = sys.network().message_count();
        let mut init_loads = HashMap::new();
        for proc in sys.process_names() {
            init_loads.insert(
                proc.clone(),
                sys.sent_message_count(&proc) + sys.received_message_count(&proc),
            );
        }

        sys.step_for_duration(10.0);

        let mut loads = Vec::new();
        for proc in sys.process_names() {
            let load = sys.sent_message_count(&proc) + sys.received_message_count(&proc);
            loads.push(load - init_loads.get(&proc).unwrap());
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        let traffic = sys.network().traffic();
        let message_count = sys.network().message_count();
        measurements.push((
            duration,
            (traffic - init_net_traffic) as f64 / duration,
            (message_count - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!(
            "- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max load - {:.2}, max/min load - {:.2}",
            sys_sizes[i], time, traffic, message_count, max_load, load_ratio
        );
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min process load")?;
    Ok(true)
}

fn test_scalability_crash(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.process_count,
        config.process_count * 2,
        config.process_count * 5,
        config.process_count * 10,
    ];
    let mut measurements = Vec::new();
    for size in sys_sizes {
        let mut run_config = config.clone();
        run_config.process_count = size;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.process_names();
        group.shuffle(&mut rand);
        let seed = &group[0];
        initialize_group(&mut sys, &group, seed)?;

        let init_time = sys.time();
        let init_net_traffic = sys.network().traffic();
        let init_msg_count = sys.network().message_count();
        let mut init_loads = HashMap::new();
        for proc in sys.process_names() {
            init_loads.insert(
                proc.clone(),
                sys.sent_message_count(&proc) + sys.received_message_count(&proc),
            );
        }

        let crashed = group.remove(rand.gen_range(0..group.len()));
        crash_process(&crashed, &mut sys);
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

        let mut loads = Vec::new();
        for proc in sys.process_names() {
            if proc != crashed {
                let load = sys.sent_message_count(&proc) + sys.received_message_count(&proc);
                loads.push(load - init_loads.get(&proc).unwrap());
            }
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        let traffic = sys.network().traffic();
        let message_count = sys.network().message_count();
        measurements.push((
            duration,
            (traffic - init_net_traffic) as f64 / duration,
            (message_count - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!(
            "- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max load - {:.2}, max/min load - {:.2}",
            sys_sizes[i], time, traffic, message_count, max_load, load_ratio
        );
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min process load")?;
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Membership Homework Tests
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

    /// Random seed
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of processes
    #[clap(long, short, default_value = "10")]
    process_count: u32,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "100")]
    monkeys: u32,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let process_factory = PyProcessFactory::new(&args.solution_path, "GroupMember");
    let config = TestConfig {
        process_factory: &process_factory,
        process_count: args.process_count,
        seed: args.seed,
    };
    let mut tests = TestSuite::new();

    tests.add("SIMPLE", test_simple, config.clone());
    tests.add("RANDOM SEED", test_random_seed, config.clone());
    tests.add("PROCESS JOIN", test_process_join, config.clone());
    tests.add("PROCESS LEAVE", test_process_leave, config.clone());
    tests.add("PROCESS CRASH", test_process_crash, config.clone());
    tests.add("SEED PROCESS CRASH", test_seed_process_crash, config.clone());
    tests.add("PROCESS CRASH RECOVER", test_process_crash_recover, config.clone());
    tests.add("PROCESS OFFLINE", test_process_offline, config.clone());
    tests.add("SEED PROCESS OFFLINE", test_seed_process_offline, config.clone());
    tests.add("PROCESS OFFLINE RECOVER", test_process_offline_recover, config.clone());
    tests.add("PROCESS CANNOT RECEIVE", test_process_cannot_receive, config.clone());
    tests.add("PROCESS CANNOT SEND", test_process_cannot_send, config.clone());
    tests.add("NETWORK PARTITION", test_network_partition, config.clone());
    tests.add(
        "NETWORK PARTITION RECOVER",
        test_network_partition_recover,
        config.clone(),
    );
    tests.add(
        "TWO PROCESSES CANNOT COMMUNICATE",
        test_two_processes_cannot_communicate,
        config.clone(),
    );
    tests.add("SLOW NETWORK", test_slow_network, config.clone());
    tests.add("FLAKY NETWORK", test_flaky_network, config.clone());
    tests.add("FLAKY NETWORK ON START", test_flaky_network_on_start, config.clone());
    tests.add("FLAKY NETWORK AND CRASH", test_flaky_network_and_crash, config.clone());
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for run in 1..=args.monkeys {
        let mut run_config = config.clone();
        run_config.seed = rand.next_u64();
        tests.add(&format!("CHAOS MONKEY (run {})", run), test_chaos_monkey, run_config);
    }
    tests.add("SCALABILITY NORMAL", test_scalability_normal, config.clone());
    tests.add("SCALABILITY CRASH", test_scalability_crash, config.clone());

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
