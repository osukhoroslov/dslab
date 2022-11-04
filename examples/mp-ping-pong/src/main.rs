mod basic;
mod retry;

use std::env;
use std::io::Write;

use assertables::assume;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::node::ProcessEvent;
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
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    let server_node = sys.add_node("server");
    let client_node = sys.add_node("client");
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
    server_node.borrow_mut().add_process("server", server);
    client_node.borrow_mut().add_process("client", client);
    return sys;
}

fn get_local_messages(sys: &System, proc: &str) -> Vec<Message> {
    let mut messages = Vec::new();
    for e in sys.event_log(proc) {
        match e.event {
            ProcessEvent::LocalMessageSent { msg } => {
                messages.push(msg);
            }
            _ => {}
        }
    }
    messages
}

fn check_messages(messages: Vec<Message>, expected: Vec<String>) -> TestResult {
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == expected.len(), "Wrong number of messages!")?;
    for i in 0..messages.len() {
        let m = messages.get(i).unwrap();
        let e = expected.get(i).unwrap();
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == *e, "Wrong message data!")?;
    }
    Ok(true)
}

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_run(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.step_until_no_events();
    Ok(true)
}

fn test_result(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", data);
    sys.send_local(ping, "client");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_result_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_drop_rate(0.5);
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", data);
    sys.send_local(ping, "client");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_10results_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_drop_rate(0.5);
    let data = r#"{"value": "Hello!"}"#;
    for i in 0..10 {
        let ping = Message::new("PING", data);
        sys.send_local(ping, "client");
        sys.step_until_no_events();
        let messages = get_local_messages(&sys, "client");
        check_messages(messages, vec![data.to_string(); i + 1])?;
    }
    Ok(true)
}

fn test_drop_ping(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_drop_rate(1.0);
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.steps(10);
    sys.network().borrow_mut().set_drop_rate(0.0);
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_drop_pong(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.network().borrow_mut().set_drop_rate(1.0);
    sys.steps(10);
    sys.network().borrow_mut().set_drop_rate(0.0);
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_drop_ping2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().drop_outgoing("client");
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", data);
    sys.send_local(ping, "client");
    sys.steps(10);
    sys.network().borrow_mut().pass_outgoing("client");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_drop_pong2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().drop_outgoing("server");
    let data = r#"{"value": "Hello!"}"#;
    let ping = Message::new("PING", data);
    sys.send_local(ping, "client");
    sys.steps(10);
    sys.network().borrow_mut().pass_outgoing("server");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    check_messages(messages, vec![data.to_string()])
}

fn test_10results_unique(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_delays(1.0, 2.0);
    let mut expected = Vec::new();
    for i in 0..10 {
        let data = format!(r#"{{"value": "Hello{}!"}}"#, i);
        let ping = Message::new("PING", &data);
        sys.send_local(ping, "client");
        sys.step_until_local_message("client")?;
        expected.push(data);
        let messages = get_local_messages(&sys, "client");
        check_messages(messages, expected.clone())?;
    }
    Ok(true)
}

fn test_10results_unique_unreliable(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_delays(1.0, 2.0);
    sys.network().borrow_mut().set_drop_rate(0.5);
    let mut expected = Vec::new();
    for i in 0..10 {
        let data = format!(r#"{{"value": "Hello{}!"}}"#, i);
        let ping = Message::new("PING", &data);
        sys.send_local(ping, "client");
        sys.step_until_local_message("client")?;
        expected.push(data);
        let messages = get_local_messages(&sys, "client");
        check_messages(messages, expected.clone())?;
    }
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
    tests.add("10 UNIQUE RESULTS UNRELIABLE", test_10results_unique, config.clone());

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
