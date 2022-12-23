use std::collections::HashMap;
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// UTILS ---------------------------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    impl_path: &'a str,
    sender_class: &'a str,
    receiver_class: &'a str,
    seed: u64,
    monkeys: u32,
    reliable: bool,
    once: bool,
    ordered: bool,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(Some("dslab_mp"), level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig, measure_max_size: bool) -> System {
    let mut sys = System::new(config.seed);
    sys.add_node("sender-node");
    sys.add_node("receiver-node");

    let sender_f = PyProcessFactory::new(config.impl_path, config.sender_class);
    let mut sender = sender_f.build(("sender", "receiver"), config.seed);
    if measure_max_size {
        sender.set_max_size_freq(100);
    }
    sys.add_process("sender", boxed!(sender), "sender-node");

    let receiver_f = PyProcessFactory::new(config.impl_path, config.receiver_class);
    let mut receiver = receiver_f.build(("receiver",), config.seed);
    if measure_max_size {
        receiver.set_max_size_freq(100);
    }
    sys.add_process("receiver", boxed!(receiver), "receiver-node");

    sys
}

fn generate_message_texts(sys: &mut System, message_count: usize) -> Vec<String> {
    if message_count == 5 {
        ["distributed", "systems", "need", "some", "guarantees"]
            .map(String::from)
            .to_vec()
    } else {
        let mut messages = Vec::new();
        for _i in 0..message_count {
            let msg = if message_count == 10 {
                format!("{}C", sys.gen_range(20..30))
            } else {
                sys.random_string(100)
            };
            messages.push(msg);
        }
        messages
    }
}

fn send_messages(sys: &mut System, message_count: usize) -> Vec<Message> {
    let texts = generate_message_texts(sys, message_count);
    let mut messages = Vec::new();
    for text in texts {
        let msg = Message::new("MESSAGE", &format!(r#"{{"text": "{}"}}"#, text));
        sys.send_local_message("sender", msg.clone());
        let steps = if message_count <= 10 {
            sys.gen_range(0..2)
        } else {
            sys.gen_range(0..7)
        };
        if steps > 0 {
            sys.steps(steps);
        }
        messages.push(msg);
    }
    messages
}

fn check_guarantees(sys: &mut System, sent: &[Message], config: &TestConfig) -> TestResult {
    let mut msg_count = HashMap::new();
    let mut expected_msg_count = HashMap::new();
    for msg in sent {
        msg_count.insert(msg.data.clone(), 0);
        *expected_msg_count.entry(&msg.data).or_insert(0) += 1;
    }
    let delivered = sys.read_local_messages("receiver");
    // check that delivered messages have expected type and data
    for msg in delivered.iter() {
        // assuming all messages have the same type
        assume_eq!(msg.tip, sent[0].tip, format!("Wrong message type {}", msg.tip))?;
        assume!(
            msg_count.contains_key(&msg.data),
            format!("Wrong message data: {}", msg.data)
        )?;
        *msg_count.get_mut(&msg.data).unwrap() += 1;
    }
    // check delivered message count according to expected guarantees
    for (data, count) in msg_count {
        let expected_count = expected_msg_count[&data];
        assume!(
            count >= expected_count || !config.reliable,
            format!(
                "Message {} is not delivered (observed count {} < expected count {})",
                data, count, expected_count
            )
        )?;
        assume!(
            count <= expected_count || !config.once,
            format!(
                "Message {} is delivered more than once (observed count {} > expected count {})",
                data, count, expected_count
            )
        )?;
    }
    // check message delivery order
    if config.ordered {
        let mut next_idx = 0;
        for i in 0..delivered.len() {
            let msg = &delivered[i];
            let mut matched = false;
            while !matched && next_idx < sent.len() {
                if msg.data == sent[next_idx].data {
                    matched = true;
                } else {
                    next_idx += 1;
                }
            }
            assume!(
                matched,
                format!("Order violation: {} after {}", msg.data, &delivered[i - 1].data)
            )?;
        }
    }
    Ok(true)
}

fn check_overhead(
    guarantee: &str,
    faulty: bool,
    message_count: usize,
    sender_mem: u64,
    receiver_mem: u64,
    net_message_count: u64,
    net_traffic: u64,
) -> TestResult {
    let (sender_mem_limit, receiver_mem_limit, net_message_count_limit, net_traffic_limit) = match guarantee {
        "AMO" => match message_count {
            100 => {
                if !faulty {
                    (500, 1000, 100, 15000)
                } else {
                    (500, 3000, 100, 15000)
                }
            }
            1000 => {
                if !faulty {
                    (500, 1000, 1000, 150000)
                } else {
                    (500, 30000, 1000, 150000)
                }
            }
            _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        },
        "ALO" => match message_count {
            100 => {
                if !faulty {
                    (2000, 300, 200, 15000)
                } else {
                    (30000, 300, 500, 30000)
                }
            }
            1000 => {
                if !faulty {
                    (10000, 300, 2000, 150000)
                } else {
                    (400000, 300, 5000, 300000)
                }
            }
            _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        },
        "EO" => match message_count {
            100 => {
                if !faulty {
                    (2000, 1000, 200, 15000)
                } else {
                    (30000, 2000, 500, 30000)
                }
            }
            1000 => {
                if !faulty {
                    (10000, 1000, 2000, 150000)
                } else {
                    (400000, 20000, 5000, 300000)
                }
            }
            _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        },
        "EOO" => match message_count {
            100 => {
                if !faulty {
                    (3000, 1000, 200, 16000)
                } else {
                    (20000, 6000, 500, 30000)
                }
            }
            1000 => {
                if !faulty {
                    (10000, 1000, 2000, 200000)
                } else {
                    (300000, 10000, 5000, 400000)
                }
            }
            _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
        },
        _ => (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
    };
    assume!(
        sender_mem <= sender_mem_limit,
        format!("Sender memory > {}", sender_mem_limit)
    )?;
    assume!(
        receiver_mem <= receiver_mem_limit,
        format!("Receiver memory > {}", receiver_mem_limit)
    )?;
    assume!(
        net_message_count <= net_message_count_limit,
        format!("Message count > {}", net_message_count_limit)
    )?;
    assume!(
        net_traffic <= net_traffic_limit,
        format!("Traffic > {}", net_traffic_limit)
    )?;
    Ok(true)
}

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_normal(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let messages = send_messages(&mut sys, 5);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)?;
    // we expect no more than 5 messages from sender in ideal network conditions
    let sent_count = sys.sent_message_count("sender");
    assume!(
        sent_count <= 5,
        format!("Sender sent {} messages, expected at most 5", sent_count)
    )
}

fn test_normal_non_unique(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let messages = send_messages(&mut sys, 10);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)?;
    // we expect no more than 10 messages from sender in ideal network conditions
    let sent_count = sys.sent_message_count("sender");
    assume!(
        sent_count <= 10,
        format!("Sender sent {} messages, expected at most 10", sent_count)
    )
}

fn test_delayed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().borrow_mut().set_delays(1., 3.);
    let messages = send_messages(&mut sys, 5);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)
}

fn test_duplicated(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().borrow_mut().set_dupl_rate(0.3);
    let messages = send_messages(&mut sys, 5);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)
}

fn test_delayed_duplicated(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().borrow_mut().set_delays(1., 3.);
    sys.network().borrow_mut().set_dupl_rate(0.3);
    let messages = send_messages(&mut sys, 5);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)
}

fn test_dropped(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    sys.network().borrow_mut().set_drop_rate(0.3);
    let messages = send_messages(&mut sys, 5);
    sys.step_until_no_events();
    check_guarantees(&mut sys, &messages, config)
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for i in 1..=config.monkeys {
        let mut run_config = *config;
        run_config.seed = rand.next_u64();
        println!("Run {} (seed: {})", i, run_config.seed);
        let mut sys = build_system(&run_config, false);
        sys.network().borrow_mut().set_delays(1., 3.);
        sys.network().borrow_mut().set_dupl_rate(0.3);
        sys.network().borrow_mut().set_drop_rate(0.3);
        let messages = send_messages(&mut sys, 10);
        sys.step_until_no_events();
        let res = check_guarantees(&mut sys, &messages, &run_config);
        res.as_ref()?;
    }
    Ok(true)
}

fn test_overhead(config: &TestConfig, guarantee: &str, faulty: bool) -> TestResult {
    for message_count in [100, 500, 1000] {
        let mut sys = build_system(config, true);
        if faulty {
            sys.network().borrow_mut().set_delays(1., 3.);
            sys.network().borrow_mut().set_dupl_rate(0.3);
            sys.network().borrow_mut().set_drop_rate(0.3);
        }
        let messages = send_messages(&mut sys, message_count);
        sys.step_until_no_events();
        let res = check_guarantees(&mut sys, &messages, config);
        res.as_ref()?;
        let sender_mem = sys.max_size("sender");
        let receiver_mem = sys.max_size("receiver");
        let net_message_count = sys.network().borrow().message_count();
        let net_traffic = sys.network().borrow().traffic();
        println!(
            "{:<6} Send Mem: {:<8} Recv Mem: {:<8} Messages: {:<8} Traffic: {}",
            message_count, sender_mem, receiver_mem, net_message_count, net_traffic
        );
        check_overhead(
            guarantee,
            faulty,
            message_count,
            sender_mem,
            receiver_mem,
            net_message_count,
            net_traffic,
        )?;
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Guarantees Homework Tests
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

    /// Guarantee to check
    #[clap(long, short, possible_values = ["AMO", "ALO", "EO", "EOO"])]
    guarantee: Option<String>,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "0")]
    monkeys: u32,

    /// Run overhead tests
    #[clap(long, short)]
    overhead: bool,
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Debug);
    }
    let guarantee = args.guarantee.as_deref();

    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    let mut config = TestConfig {
        impl_path: &args.solution_path,
        sender_class: "",
        receiver_class: "",
        seed: args.seed,
        monkeys: args.monkeys,
        reliable: false,
        once: false,
        ordered: false,
    };
    let mut tests = TestSuite::new();

    // At most once
    if guarantee.is_none() || guarantee == Some("AMO") {
        config.sender_class = "AtMostOnceSender";
        config.receiver_class = "AtMostOnceReceiver";
        config.once = true;
        // without drops should be reliable
        config.reliable = true;
        tests.add("[AT MOST ONCE] NORMAL", test_normal, config);
        tests.add("[AT MOST ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[AT MOST ONCE] DELAYED", test_delayed, config);
        tests.add("[AT MOST ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[AT MOST ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        // with drops is not reliable
        config.reliable = false;
        tests.add("[AT MOST ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[AT MOST ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            config.reliable = true;
            tests.add(
                "[AT MOST ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "AMO", false),
                config,
            );
            config.reliable = false;
            tests.add(
                "[AT MOST ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "AMO", true),
                config,
            );
        }
    }

    // At least once
    if guarantee.is_none() || guarantee == Some("ALO") {
        config.sender_class = "AtLeastOnceSender";
        config.receiver_class = "AtLeastOnceReceiver";
        config.reliable = true;
        config.once = false;
        tests.add("[AT LEAST ONCE] NORMAL", test_normal, config);
        tests.add("[AT LEAST ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[AT LEAST ONCE] DELAYED", test_delayed, config);
        tests.add("[AT LEAST ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[AT LEAST ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        tests.add("[AT LEAST ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[AT LEAST ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[AT LEAST ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "ALO", false),
                config,
            );
            tests.add(
                "[AT LEAST ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "ALO", true),
                config,
            );
        }
    }

    // Exactly once
    if guarantee.is_none() || guarantee == Some("EO") {
        config.sender_class = "ExactlyOnceSender";
        config.receiver_class = "ExactlyOnceReceiver";
        config.reliable = true;
        config.once = true;
        tests.add("[EXACTLY ONCE] NORMAL", test_normal, config);
        tests.add("[EXACTLY ONCE] NORMAL NON-UNIQUE", test_normal_non_unique, config);
        tests.add("[EXACTLY ONCE] DELAYED", test_delayed, config);
        tests.add("[EXACTLY ONCE] DUPLICATED", test_duplicated, config);
        tests.add("[EXACTLY ONCE] DELAYED+DUPLICATED", test_delayed_duplicated, config);
        tests.add("[EXACTLY ONCE] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[EXACTLY ONCE] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[EXACTLY ONCE] OVERHEAD NORMAL",
                |x| test_overhead(x, "EO", false),
                config,
            );
            tests.add(
                "[EXACTLY ONCE] OVERHEAD FAULTY",
                |x| test_overhead(x, "EO", true),
                config,
            );
        }
    }

    // EXACTLY ONCE ORDERED
    if guarantee.is_none() || guarantee == Some("EOO") {
        config.sender_class = "ExactlyOnceOrderedSender";
        config.receiver_class = "ExactlyOnceOrderedReceiver";
        config.reliable = true;
        config.once = true;
        config.ordered = true;
        tests.add("[EXACTLY ONCE ORDERED] NORMAL", test_normal, config);
        tests.add(
            "[EXACTLY ONCE ORDERED] NORMAL NON-UNIQUE",
            test_normal_non_unique,
            config,
        );
        tests.add("[EXACTLY ONCE ORDERED] DELAYED", test_delayed, config);
        tests.add("[EXACTLY ONCE ORDERED] DUPLICATED", test_duplicated, config);
        tests.add(
            "[EXACTLY ONCE ORDERED] DELAYED+DUPLICATED",
            test_delayed_duplicated,
            config,
        );
        tests.add("[EXACTLY ONCE ORDERED] DROPPED", test_dropped, config);
        if args.monkeys > 0 {
            tests.add("[EXACTLY ONCE ORDERED] CHAOS MONKEY", test_chaos_monkey, config);
        }
        if args.overhead {
            tests.add(
                "[EXACTLY ONCE ORDERED] OVERHEAD NORMAL",
                |x| test_overhead(x, "EOO", false),
                config,
            );
            tests.add(
                "[EXACTLY ONCE ORDERED] OVERHEAD FAULTY",
                |x| test_overhead(x, "EOO", true),
                config,
            );
        }
    }

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
