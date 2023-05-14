mod basic;
mod retry;


use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::env;
use std::io::Write;

use assertables::assume;
use clap::Parser;
use dslab_mp::mc::events::McEvent;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::system::McState;
use env_logger::Builder;
use log::LevelFilter;
use sugars::boxed;

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

fn mc_goal<'a>() -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(|state| {
        if state.node_states["client-node"]["client"].local_outbox.len() == 2 {
            Some("got two messages".to_owned())
        } else {
            None
        }
    })
} 

fn mc_check_received_messages(state: &McState, messages_expected: &HashSet<String>) -> Result<(), String> {
    let mut messages_got = HashSet::<String>::default();
    for message in &state.node_states["client-node"]["client"].local_outbox {
        if !messages_got.insert(message.data.clone()) {
            return Err("result duplicated".to_owned());
        }
        if !messages_expected.contains(&message.data) {
            return Err("this result was not expected".to_owned());
        }
    }
    Ok(())
}

fn mc_check_too_deep(state: &McState, depth: u64) -> Result<(), String> {
    if state.search_depth > depth {
        Err("too deep".to_owned())
    } else {
        Ok(())
    }
}

fn mc_prune_number_of_drops(state: &McState, drops_allowed: u64) -> Option<String> {
    let drops = state.log.iter().filter(|event| 
        if let McEvent::MessageDropped{..} = **event {
            true
        } else {
            false
        }
    ).count();
    if drops > drops_allowed as usize {
        Some("too many dropped messages".to_owned())
    } else {
        None
    }
}

fn mc_prune_many_messages_sent(state: &McState, allowed: u64) -> Option<String> {
    if state.node_states["client-node"]["client"].sent_message_count > allowed {
        Some("too many messages sent from client".to_owned())
    } else if state.node_states["server-node"]["server"].sent_message_count > allowed {
        Some("too many messages sent from server".to_owned())
    } else {
        None
    }
}

fn test_mc_reliable_network(config: &TestConfig) -> TestResult {
    let mut system = build_system(config);
    let data = format!(r#"{{"value": 0}}"#);
    let data2 = format!(r#"{{"value": 1}}"#);
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);
    system.send_local_message("client", Message::new("PING", &data.clone()));
    system.send_local_message("client", Message::new("PING", &data2.clone()));
    let mut mc = ModelChecker::new(&system, Box::new(Dfs::new(
        Box::new(|state|{
            mc_prune_many_messages_sent(state, 4)
        }),        
        mc_goal(),
        Box::new(|state|{
            mc_check_received_messages(state, &messages_expected)?;
            mc_check_too_deep(state, 20)?;
            Ok(())
        }),
        None,
     dslab_mp::mc::strategy::ExecutionMode::Debug,
    )));
    let res = mc.run();
    assume!(
        res.is_ok(),
        format!("model checher found error: {}", res.as_ref().err().unwrap())
    )?;
    println!("{:?}", res.unwrap());
    Ok(true)
}

fn test_mc_unreliable_network(config: &TestConfig) -> TestResult {
    let mut system = build_system(config);
    let data = format!(r#"{{"value": 0}}"#);
    let data2 = format!(r#"{{"value": 1}}"#);
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);
    system.send_local_message("client", Message::new("PING", &data.clone()));
    system.send_local_message("client", Message::new("PING", &data2.clone()));
    system.network().borrow_mut().set_drop_rate(0.3);
    let mut mc = ModelChecker::new(&system, Box::new(Dfs::new(
        Box::new(|state| {
            mc_check_too_deep(state, 7).err()
        }),
        mc_goal(),
        Box::new(|state|{
            mc_check_received_messages(state, &messages_expected)?;
            Ok(())
        }),None, dslab_mp::mc::strategy::ExecutionMode::Debug,
    )));
    let res = mc.run();
    assume!(
        res.is_ok(),
        format!("model checher found error: {}", res.as_ref().err().unwrap())
    )?;
    println!("{:?}", res.unwrap());
    Ok(true)
}

fn test_mc_limited_drops(config: &TestConfig) -> TestResult {
    let mut system = build_system(config);
    let data = format!(r#"{{"value": 0}}"#);
    let data2 = format!(r#"{{"value": 1}}"#);
    let messages_expected = HashSet::<String>::from_iter([data.clone(), data2.clone()]);
    system.send_local_message("client", Message::new("PING", &data.clone()));
    system.send_local_message("client", Message::new("PING", &data2.clone()));
    system.network().borrow_mut().set_drop_rate(0.3);
    let mut mc = ModelChecker::new(&system, Box::new(Dfs::new(
        Box::new(|state| {
            let num_drops_allowed = 3;
            mc_prune_number_of_drops(state, num_drops_allowed).or_else(||
                mc_prune_many_messages_sent(state, num_drops_allowed + 2)
            )
        }),
        mc_goal(),
        Box::new(|state|{
            mc_check_received_messages(state, &messages_expected)?;
            mc_check_too_deep(state, 20)?;
            Ok(())
        }),None, dslab_mp::mc::strategy::ExecutionMode::Debug,
    )));
    let res = mc.run();
    assume!(
        res.is_ok(),
        format!("model checher found error: {}", res.as_ref().err().unwrap())
    )?;
    println!("{:?}", res.unwrap());
    Ok(true)
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
    tests.add("10 UNIQUE RESULTS UNRELIABLE", test_10results_unique_unreliable, config.clone());
    tests.add("MODEL CHECKING", test_mc_reliable_network, config.clone());
    tests.add("MODEL CHECKING UNRELIABLE", test_mc_unreliable_network, config.clone());
    tests.add("MODEL CHECKING UNRELIABLE WITH LIMITS", test_mc_limited_drops, config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
