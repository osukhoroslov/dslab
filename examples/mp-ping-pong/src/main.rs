mod basic;
mod retry;

use std::collections::HashSet;
use std::env;
use std::io::Write;

use assertables::assume;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use sugars::boxed;

use dslab_mp::logger::LogEntry;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::predicates::{goals, invariants, prunes};
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategy::StrategyConfig;
use dslab_mp::message::Message;
use dslab_mp::process::Process;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

use crate::basic::client::BasicPingClient;
use crate::basic::server::BasicPingServer;
use crate::retry::client::RetryPingClient;
use crate::retry::server::RetryPingServer;

// UTILS ---------------------------------------------------------------------------------------------------------------

#[derive(Clone)]
struct TestConfig {
    impl_path: String,
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
    sys.add_node("server-node");
    sys.add_node("client-node");
    let (server, client): (Box<dyn Process>, Box<dyn Process>) = match config.impl_path.as_str() {
        "basic" => (boxed!(BasicPingServer {}), boxed!(BasicPingClient::new("server"))),
        "retry" => (boxed!(RetryPingServer {}), boxed!(RetryPingClient::new("server"))),
        _ => {
            let server_f = PyProcessFactory::new(&config.impl_path, "PingServer");
            let server = server_f.build(("server",), config.seed);
            let client_f = PyProcessFactory::new(&config.impl_path, "PingClient");
            let client = client_f.build(("client", "server"), config.seed);
            (boxed!(server), boxed!(client))
        }
    };
    sys.add_process("server", server, "server-node");
    sys.add_process("client", client, "client-node");
    sys
}

fn check(messages: Vec<Message>, expected: &str) -> TestResult {
    assume!(!messages.is_empty(), "No messages returned by the client")?;
    assume!(
        messages.len() == 1,
        format!("Wrong number of messages: {}", messages.len())
    )?;
    let m = &messages[0];
    assume!(m.tip == "PONG", format!("Wrong message type: {}", m.tip))?;
    assume!(
        m.data == expected,
        format!("Wrong message data: {}, expected: {}", m.data, expected)
    )?;
    Ok(true)
}

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_run(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local_message("client", msg);
    sys.step_until_no_events();
    Ok(true)
}

fn test_result(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_result_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_drop_rate(0.5);
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_10results_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_drop_rate(0.5);
    let data = r#"{"value": "Hello!"}"#;
    for _ in 0..10 {
        sys.send_local_message("client", Message::new("PING", data));
        sys.step_until_no_events();
        check(sys.read_local_messages("client"), data)?;
    }
    Ok(true)
}

fn test_drop_ping(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_drop_rate(1.0);
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.steps(10);
    sys.network().set_drop_rate(0.0);
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_drop_pong(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.network().set_drop_rate(1.0);
    sys.steps(10);
    sys.network().set_drop_rate(0.0);
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_drop_ping2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().drop_outgoing("client-node");
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.steps(10);
    sys.network().pass_outgoing("client-node");
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_drop_pong2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().drop_outgoing("server-node");
    let data = r#"{"value": "Hello!"}"#;
    sys.send_local_message("client", Message::new("PING", data));
    sys.steps(10);
    sys.network().pass_outgoing("server-node");
    sys.step_until_no_events();
    check(sys.read_local_messages("client"), data)
}

fn test_10results_unique(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_delays(1.0, 2.0);
    for i in 0..10 {
        let data = format!(r#"{{"value": "Hello{}!"}}"#, i);
        sys.send_local_message("client", Message::new("PING", &data));
        let messages = sys.step_until_local_message("client")?;
        check(messages, &data)?;
    }
    Ok(true)
}

fn test_10results_unique_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_delays(1.0, 2.0);
    sys.network().set_drop_rate(0.5);
    for i in 0..10 {
        let data = format!(r#"{{"value": "Hello{}!"}}"#, i);
        sys.send_local_message("client", Message::new("PING", &data));
        let messages = sys.step_until_local_message("client")?;
        check(messages, &data)?;
    }
    Ok(true)
}

// MODEL CHECKING ------------------------------------------------------------------------------------------------------

fn test_mc_reliable_network(config: &TestConfig) -> TestResult {
    let system = build_system(config);
    let data = r#"{"value": 0}"#.to_string();
    let data2 = r#"{"value": 1}"#.to_string();
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);

    let strategy_config = StrategyConfig::default()
        .prune(prunes::mc_prune_sent_messages_limit(4))
        .goal(goals::mc_goal_got_n_local_messages("client-node", "client", 2))
        .invariant(invariants::mc_all_invariants(vec![
            invariants::mc_invariant_received_messages("client-node", "client", messages_expected),
            invariants::mc_invariant_state_depth(20),
        ]));

    let mut mc = ModelChecker::new::<Dfs>(&system, strategy_config);
    let res = mc.run_with_change(|system| {
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data),
        );
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data2),
        );
    });

    if let Err(e) = res {
        println!("{:#?}", e);
        Err(e.message())
    } else {
        Ok(true)
    }
}

fn test_mc_unreliable_network(config: &TestConfig) -> TestResult {
    let system = build_system(config);
    let data = r#"{"value": 0}"#.to_string();
    let data2 = r#"{"value": 1}"#.to_string();
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);
    system.network().set_drop_rate(0.3);
    let strategy_config = StrategyConfig::default()
        .prune(prunes::mc_prune_state_depth(7))
        .goal(goals::mc_goal_got_n_local_messages("client-node", "client", 2))
        .invariant(invariants::mc_invariant_received_messages(
            "client-node",
            "client",
            messages_expected,
        ));
    let mut mc = ModelChecker::new::<Dfs>(&system, strategy_config);

    let res = mc.run_with_change(|system| {
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data),
        );
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data2),
        );
    });

    if let Err(e) = res {
        println!("{:#?}", e);
        Err(e.message())
    } else {
        Ok(true)
    }
}

fn test_mc_limited_message_drops(config: &TestConfig) -> TestResult {
    let sys = build_system(config);
    sys.network().set_drop_rate(0.1);
    let data = r#"{"value": 0}"#.to_string();
    let data2 = r#"{"value": 1}"#.to_string();
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);
    let num_drops_allowed = 3;
    let strategy_config = StrategyConfig::default()
        .prune(prunes::mc_any_prune(vec![
            prunes::mc_prune_events_limit(LogEntry::is_mc_message_dropped, num_drops_allowed),
            prunes::mc_prune_events_limit(LogEntry::is_mc_message_sent, 2 + num_drops_allowed),
        ]))
        .goal(goals::mc_goal_got_n_local_messages("client-node", "client", 2))
        .invariant(invariants::mc_invariant_received_messages(
            "client-node",
            "client",
            messages_expected,
        ));
    let mut mc = ModelChecker::new::<Dfs>(&sys, strategy_config);

    let res = mc.run_with_change(|system| {
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data),
        );
        system.send_local_message(
            "client-node".to_string(),
            "client".to_string(),
            Message::new("PING", &data2),
        );
    });

    if let Err(e) = res {
        println!("{:#?}", e);
        Err(e.message())
    } else {
        Ok(true)
    }
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Ping-Pong Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with PingClient and PingServer implementations
    /// or name of Rust implementation (basic or retry)
    #[clap(long = "impl", short)]
    impl_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.impl_path.ends_with(".py") {
        env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    }
    init_logger(LevelFilter::Debug);
    let config = TestConfig {
        impl_path: args.impl_path,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("RUN", test_run, config.clone());
    tests.add("RESULT", test_result, config.clone());
    tests.add("RESULT UNRELIABLE", test_result_unreliable, config.clone());
    tests.add("10 RESULTS UNRELIABLE", test_10results_unreliable, config.clone());
    tests.add("DROP PING", test_drop_ping, config.clone());
    tests.add("DROP PONG", test_drop_pong, config.clone());
    tests.add("DROP PING 2", test_drop_ping2, config.clone());
    tests.add("DROP PONG 2", test_drop_pong2, config.clone());
    tests.add("10 UNIQUE RESULTS", test_10results_unique, config.clone());
    tests.add(
        "10 UNIQUE RESULTS UNRELIABLE",
        test_10results_unique_unreliable,
        config.clone(),
    );
    tests.add("MODEL CHECKING", test_mc_reliable_network, config.clone());
    tests.add("MODEL CHECKING UNRELIABLE", test_mc_unreliable_network, config.clone());
    tests.add("MODEL CHECKING LIMITED DROPS", test_mc_limited_message_drops, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
