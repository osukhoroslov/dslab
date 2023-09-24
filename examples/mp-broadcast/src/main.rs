use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;
use std::time::Duration;

use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::Serialize;
use serde_json::Value;
use sugars::boxed;

use dslab_mp::logger::LogEntry;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::predicates::{collects, goals, invariants, prunes};
use dslab_mp::mc::state::McState;
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategy::{GoalFn, InvariantFn, PruneFn, StrategyConfig};
use dslab_mp::message::Message;
use dslab_mp::node::ProcessEvent;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// UTILS -------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    proc_factory: &'a PyProcessFactory,
    proc_count: u64,
    seed: u64,
    monkeys: u32,
    debug: bool,
}

#[derive(Serialize)]
struct BroadcastMessage<'a> {
    text: &'a str,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    let mut proc_names = Vec::new();
    for n in 0..config.proc_count {
        proc_names.push(format!("{}", n));
    }
    for proc_name in &proc_names {
        let proc = config.proc_factory.build((proc_name, proc_names.clone()), config.seed);
        // process and node on which it runs have the same name
        sys.add_node(proc_name);
        sys.add_process(proc_name, boxed!(proc), proc_name);
    }
    sys
}

fn check(sys: &System, config: &TestConfig, all_sent_messages: HashSet<String>) -> TestResult {
    let mut sent = HashMap::new();
    let mut delivered = HashMap::new();
    let mut all_delivered = HashSet::new();
    let mut histories = HashMap::new();
    for proc in sys.process_names() {
        let mut history = Vec::new();
        let mut sent_msgs = Vec::new();
        let mut delivered_msgs = Vec::new();
        for e in sys.event_log(&proc) {
            match e.event {
                ProcessEvent::LocalMessageReceived { msg: m } => {
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    sent_msgs.push(message.clone());
                    history.push(message);
                }
                ProcessEvent::LocalMessageSent { msg: m } => {
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    delivered_msgs.push(message.clone());
                    all_delivered.insert(message.clone());
                    history.push(message);
                }
                _ => {}
            }
        }
        sent.insert(proc.clone(), sent_msgs);
        delivered.insert(proc.clone(), delivered_msgs);
        histories.insert(proc, history);
    }

    if config.debug {
        println!(
            "Messages sent across network: {}",
            sys.network().network_message_count()
        );
        println!("Process histories:");
        for proc in sys.process_names() {
            println!("- [{}] {}", proc, histories.get(&proc).unwrap().join(", "));
        }
    }

    // NO DUPLICATION
    let mut no_duplication = true;
    for delivered_msgs in delivered.values() {
        let mut uniq = HashSet::new();
        for msg in delivered_msgs {
            if uniq.contains(msg) {
                println!("Message '{}' is duplicated!", msg);
                no_duplication = false;
            };
            uniq.insert(msg);
        }
    }

    // NO CREATION
    let mut no_creation = true;
    for delivered_msgs in delivered.values() {
        for msg in delivered_msgs {
            if !all_sent_messages.contains(msg) {
                println!("Message '{}' was not sent!", msg);
                no_creation = false;
            }
        }
    }

    // VALIDITY
    let mut validity = true;
    for (proc, sent_msgs) in &sent {
        if sys.proc_node_is_crashed(proc) {
            continue;
        }
        let delivered_msgs = delivered.get(proc).unwrap();
        for msg in sent_msgs {
            if !delivered_msgs.contains(msg) {
                println!("Process {} has not delivered its own message '{}'!", proc, msg);
                validity = false;
            }
        }
    }

    // UNIFORM AGREEMENT
    let mut uniform_agreement = true;
    for msg in all_delivered.iter() {
        for (proc, delivered_msgs) in &delivered {
            if sys.proc_node_is_crashed(proc) {
                continue;
            }
            if !delivered_msgs.contains(msg) {
                println!("Message '{}' is not delivered by correct process {}!", msg, proc);
                uniform_agreement = false;
            }
        }
    }

    // CAUSAL ORDER
    let mut causal_order = true;
    for (src, sent_msgs) in &sent {
        for msg in sent_msgs.iter() {
            if !all_delivered.contains(msg) {
                continue;
            }
            // build sender past for send message event
            let mut src_past = HashSet::new();
            for e in histories.get(src).unwrap() {
                if e != msg {
                    src_past.insert(e.clone());
                } else {
                    break;
                }
            }
            // check that other correct processes have delivered all past events before delivering the message
            for (dst, delivered_msgs) in &delivered {
                if sys.proc_node_is_crashed(dst) {
                    continue;
                }
                let mut dst_past = HashSet::new();
                for e in delivered_msgs {
                    if e != msg {
                        dst_past.insert(e.clone());
                    } else {
                        break;
                    }
                }
                if !dst_past.is_superset(&src_past) {
                    let missing = src_past.difference(&dst_past).cloned().collect::<Vec<String>>();
                    println!(
                        "Causal order violation: {} not delivered [{}] before [{}]",
                        dst,
                        missing.join(", "),
                        msg
                    );
                    causal_order = false;
                }
            }
        }
    }

    if no_duplication & no_creation & validity & uniform_agreement & causal_order {
        Ok(true)
    } else {
        let mut violated = Vec::new();
        if !no_duplication {
            violated.push("NO DUPLICATION")
        }
        if !no_creation {
            violated.push("NO CREATION")
        }
        if !validity {
            violated.push("VALIDITY")
        }
        if !uniform_agreement {
            violated.push("UNIFORM AGREEMENT")
        }
        if !causal_order {
            violated.push("CAUSAL ORDER")
        }
        Err(format!("Violated {}", violated.join(", ")))
    }
}

// TESTS -------------------------------------------------------------------------------------------

fn test_normal(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let text = "0:Hello";
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text }));
    sys.step_until_no_events();
    check(&sys, config, HashSet::from([text.to_string()]))
}

fn test_sender_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let text = "0:Hello";
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text }));
    // run until the message is received by one other process
    let mut received = false;
    while !received {
        sys.step();
        for n in 1..config.proc_count {
            if sys.received_message_count(&n.to_string()) == 1 {
                received = true;
                break;
            }
        }
    }
    // crash source node
    sys.crash_node("0");
    sys.step_until_no_events();
    check(&sys, config, HashSet::from([text.to_string()]))
}

fn test_sender_crash2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let text = "0:Hello";
    // let the message to be received only by the sender itself by disconnecting it
    sys.network().disconnect_node("0");
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text }));
    sys.step();
    sys.crash_node("0");
    sys.step_until_no_events();
    check(&sys, config, HashSet::from([text.to_string()]))
}

fn test_two_crashes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    // simulate that 0 and 1 communicated only with each other and then crashed
    for n in 2..config.proc_count {
        sys.network().disconnect_node(&n.to_string());
    }
    let text = "0:Hello";
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text }));
    sys.steps(config.proc_count.pow(2));
    sys.crash_node("0");
    sys.crash_node("1");
    sys.step_until_no_events();
    check(&sys, config, HashSet::from([text.to_string()]))
}

fn test_two_crashes2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    // simulate that 1 and 2 communicated only with 0 and then crashed
    sys.network().drop_outgoing("1");
    sys.network().drop_outgoing("2");
    let text = "0:Hello";
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text }));
    sys.steps(config.proc_count.pow(2));
    sys.crash_node("1");
    sys.crash_node("2");
    sys.step_until_no_events();
    check(&sys, config, HashSet::from([text.to_string()]))
}

fn test_causal_order(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_delays(100., 200.);
    let texts = ["0:Hello", "1:How?", "0:Fine!"];
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text: texts[0] }));
    while sys.event_log("1").is_empty() {
        sys.step();
    }
    sys.network().set_delays(10., 20.);
    sys.send_local_message("1", Message::json("SEND", &BroadcastMessage { text: texts[1] }));
    while sys.event_log("0").len() < 3 {
        sys.step();
    }
    sys.network().set_delay(1.);
    sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text: texts[2] }));
    sys.step_until_no_events();
    let sent_messages = HashSet::from_iter(texts.into_iter().map(String::from));
    check(&sys, config, sent_messages)
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for i in 1..=config.monkeys {
        let mut run_config = *config;
        run_config.seed = rand.next_u64();
        println!("- Run {} (seed: {})", i, run_config.seed);
        let mut sys = build_system(config);
        let victim1 = format!("{}", rand.gen_range(0..config.proc_count));
        let mut victim2 = format!("{}", rand.gen_range(0..config.proc_count));
        while victim2 == victim1 {
            victim2 = format!("{}", rand.gen_range(0..config.proc_count));
        }
        let mut sent_messages = HashSet::new();
        for i in 0..10 {
            let sender = format!("{}", rand.gen_range(0..config.proc_count));
            let text = format!("{}:{}", sender, i);
            sent_messages.insert(text.clone());
            if i % 2 == 0 {
                sys.network().set_delays(10., 20.);
            } else {
                sys.network().set_delays(1., 2.);
            }
            for j in 0..8 {
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.network().drop_outgoing(&victim1);
                } else {
                    sys.network().pass_outgoing(&victim1);
                }
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.network().drop_outgoing(&victim2);
                } else {
                    sys.network().pass_outgoing(&victim2);
                }
                if j == 0 {
                    sys.send_local_message(&sender, Message::json("SEND", &BroadcastMessage { text: &text }));
                } else {
                    sys.step();
                }
            }
        }
        sys.crash_node(&victim1);
        sys.crash_node(&victim2);
        sys.step_until_no_events();
        check(&sys, config, sent_messages)?;
    }
    Ok(true)
}

#[allow(dead_code)]
fn test_scalability(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.proc_count,
        config.proc_count * 2,
        config.proc_count * 4,
        config.proc_count * 10,
    ];
    let mut msg_counts = Vec::new();
    for node_count in sys_sizes {
        let mut run_config = *config;
        run_config.proc_count = node_count;
        let mut sys = build_system(&run_config);
        sys.send_local_message("0", Message::json("SEND", &BroadcastMessage { text: "0:Hello!" }));
        sys.step_until_no_events();
        msg_counts.push(sys.network().network_message_count());
    }
    println!("\nMessage count:");
    for i in 0..sys_sizes.len() {
        let baseline = sys_sizes[i] * (sys_sizes[i] - 1);
        println!("- N={}: {} (baseline {})", sys_sizes[i], msg_counts[i], baseline);
    }
    Ok(true)
}

// MODEL CHECKING ------------------------------------------------------------------------------------------------------

fn mc_prune_proc_permutations(equivalent_procs: &[String]) -> PruneFn {
    let equivalent_procs = equivalent_procs.to_vec();
    boxed!(move |state| {
        let proc_names = HashSet::<String>::from_iter(equivalent_procs.clone().into_iter());
        let mut used_proc_names = HashSet::<String>::new();
        let mut waiting_for_proc = 0;
        for entry in &state.trace {
            match entry {
                LogEntry::McMessageReceived { src: proc, .. } | LogEntry::McTimerFired { proc, .. } => {
                    if used_proc_names.contains(proc) || !proc_names.contains(proc) {
                        continue;
                    }
                    if equivalent_procs[waiting_for_proc] != *proc {
                        return Some("state is the same as another state with renumerated processes".to_owned());
                    }
                    used_proc_names.insert(proc.clone());
                    waiting_for_proc += 1;
                }
                _ => {}
            }
        }
        None
    })
}

fn mc_prune_msg_per_proc_limit(proc_names: &[String], limit: usize) -> PruneFn {
    prunes::events_limit_per_proc(
        |entry: &LogEntry, proc: &String| match entry {
            LogEntry::McMessageReceived { src, .. } => src == proc,
            _ => false,
        },
        proc_names.to_owned(),
        limit,
    )
}

fn mc_invariant(proc_names: Vec<String>, sent_message: String) -> InvariantFn {
    boxed!(move |state: &McState| {
        for name in &proc_names {
            let outbox = &state.node_states[name].proc_states[name].local_outbox;
            let mut message_data = HashSet::new();
            for message in outbox {
                let data: Value = serde_json::from_str(&message.data).unwrap();
                let message = data["text"].as_str().unwrap().to_string();

                if message_data.contains(&message) {
                    return Err("No duplication violated".to_owned());
                }
                message_data.insert(message.clone());
                if message != sent_message {
                    return Err("No creation violated".to_owned());
                }
            }
        }
        Ok(())
    })
}

fn test_mc_normal_delivery(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let proc_names = sys.process_names();
    let text = "0:Hello";
    sys.send_local_message(
        proc_names[0].as_str(),
        Message::json("SEND", &BroadcastMessage { text }),
    );
    let goal = goals::all_goals(
        proc_names
            .iter()
            .map(|name| goals::got_n_local_messages(name, name, 1))
            .collect::<Vec<GoalFn>>(),
    );
    let strategy_config = StrategyConfig::default()
        .goal(goal)
        .prune(prunes::any_prune(vec![
            prunes::state_depth(10),
            mc_prune_proc_permutations(&proc_names[1..]),
            // Prune states with more than 2 messages received from any process
            mc_prune_msg_per_proc_limit(&proc_names, 2),
        ]))
        .invariant(invariants::all_invariants(vec![
            mc_invariant(proc_names.clone(), text.to_string()),
            invariants::time_limit(Duration::from_secs(100)),
        ]));
    let mut mc = ModelChecker::new(&sys);
    let res = mc.run::<Bfs>(strategy_config);
    if let Err(err) = res {
        err.print_trace();
        Err(err.message())
    } else {
        Ok(true)
    }
}

fn test_mc_sender_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let proc_names = sys.process_names();
    let text = "0:Hello";
    sys.send_local_message(
        proc_names[0].as_str(),
        Message::json("SEND", &BroadcastMessage { text }),
    );
    let goal = goals::all_goals(
        proc_names
            .iter()
            .map(|name| goals::got_n_local_messages(name, name, 1))
            .collect::<Vec<GoalFn>>(),
    );

    let strategy_config = StrategyConfig::default()
        .prune(prunes::any_prune(vec![
            prunes::state_depth(4),
            mc_prune_proc_permutations(&proc_names[1..]),
        ]))
        .goal(goal)
        .invariant(invariants::all_invariants(vec![
            mc_invariant(proc_names.clone(), text.to_string()),
            invariants::time_limit(Duration::from_secs(100)),
        ]))
        .collect(collects::any_collect(
            proc_names[1..]
                .iter()
                .map(|proc| collects::got_n_local_messages(proc, proc, 1))
                .collect(),
        ));

    let mut mc = ModelChecker::new(&sys);
    let res = mc.run::<Bfs>(strategy_config);
    let intermediate_states = res
        .map_err(|err| {
            err.print_trace();
            err.message()
        })?
        .collected_states;
    if intermediate_states.is_empty() {
        return Err("no states collected after first stage".to_string());
    }

    // Crash first node in the list
    let left_proc_names = proc_names[1..].to_vec();
    let goal = goals::all_goals(
        left_proc_names
            .iter()
            .map(|name| goals::got_n_local_messages(name, name, 1))
            .collect::<Vec<GoalFn>>(),
    );
    let strategy_config = StrategyConfig::default()
        .goal(goal)
        .invariant(invariants::all_invariants(vec![
            mc_invariant(left_proc_names, text.to_string()),
            invariants::time_limit(Duration::from_secs(100)),
        ]))
        .prune(prunes::any_prune(vec![
            prunes::state_depth(6),
            mc_prune_proc_permutations(&proc_names[1..]),
            // Prune states with more than 4 messages received from any process
            mc_prune_msg_per_proc_limit(&proc_names, 4),
        ]));
    let res = mc.run_from_states_with_change::<Bfs>(strategy_config, intermediate_states, |sys| {
        sys.crash_node(proc_names[0].clone());
    });
    if let Err(err) = res {
        err.print_trace();
        Err(err.message())
    } else {
        Ok(true)
    }
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Broadcast Homework Tests
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

    /// Random seed used in tests
    #[clap(long, short, default_value = "2023")]
    seed: u64,

    /// Number of processes
    #[clap(long, short, default_value = "5")]
    proc_count: u64,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "10")]
    monkeys: u32,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Debug);
    }

    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");

    let proc_factory = PyProcessFactory::new(&args.solution_path, "BroadcastProcess");
    let mut config = TestConfig {
        proc_factory: &proc_factory,
        proc_count: args.proc_count,
        seed: args.seed,
        monkeys: args.monkeys,
        debug: args.debug,
    };
    let mut tests = TestSuite::new();

    tests.add("NORMAL", test_normal, config);
    tests.add("SENDER CRASH", test_sender_crash, config);
    tests.add("SENDER CRASH 2", test_sender_crash2, config);
    tests.add("TWO CRASHES", test_two_crashes, config);
    tests.add("TWO CRASHES 2", test_two_crashes2, config);
    tests.add("CAUSAL ORDER", test_causal_order, config);
    tests.add("CHAOS MONKEY", test_chaos_monkey, config);
    tests.add("SCALABILITY", test_scalability, config);

    config.proc_count = 3;
    tests.add("MODEL CHECKING NORMAL DELIVERY", test_mc_normal_delivery, config);
    tests.add("MODEL CHECKING SENDER CRASH", test_mc_sender_crash, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
