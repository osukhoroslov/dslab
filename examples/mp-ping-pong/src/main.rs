mod basic;
mod retry;

use std::io::Write;

use assertables::assume;
use env_logger::Builder;
use log::LevelFilter;
use sugars::{rc, refcell};

use dslab_mp::message::Message;
use dslab_mp::node::ProcessEvent;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};

use crate::basic::client::PingClient;
use crate::basic::server::PingServer;
// use crate::retry::client::PingClient;
// use crate::retry::server::PingServer;

// UTILS -------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig {
    seed: u64,
    drop_rate: f64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    sys.add_node("server-node");
    sys.add_node("client-node");
    let server = PingServer {};
    sys.add_process(rc!(refcell!(server)), "server", "server-node");
    let client = PingClient::new("server".to_string());
    sys.add_process(rc!(refcell!(client)), "client", "client-node");
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

// TESTS -------------------------------------------------------------------------------------------

fn test_run(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.step_until_no_events();
    Ok(true)
}

fn test_result(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_drop_rate(config.drop_rate);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "More than one message???")?;
    for m in messages {
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_10results(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_drop_rate(config.drop_rate);
    for i in 0..10 {
        let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
        sys.send_local(ping, "client");
        sys.step_until_no_events();
        let messages = get_local_messages(&sys, "client");
        assume!(messages.len() > 0, "No messages returned by client!")?;
        assume!(messages.len() == 1 + i, "Wrong number of messages!")?;
        assume!(messages[i].tip == "PONG", "Wrong message type!")?;
        assume!(messages[i].data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_drop_ping(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.network().borrow_mut().set_drop_rate(1.0);
    sys.steps(10);
    sys.network().borrow_mut().set_drop_rate(0.0);
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "More than one message???")?;
    for m in messages {
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_drop_pong(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.steps(2);
    sys.network().borrow_mut().set_drop_rate(1.0);
    sys.steps(10);
    sys.network().borrow_mut().set_drop_rate(0.0);
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "More than one message???")?;
    for m in messages {
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_drop_ping2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.network().borrow_mut().drop_outgoing("client-node");
    sys.steps(10);
    sys.network().borrow_mut().pass_outgoing("client-node");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "More than one message???")?;
    for m in messages {
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_drop_pong2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let ping = Message::new("PING", r#"{"value": "Hello!"}"#);
    sys.send_local(ping, "client");
    sys.network().borrow_mut().drop_outgoing("server-node");
    sys.steps(10);
    sys.network().borrow_mut().pass_outgoing("server-node");
    sys.step_until_no_events();
    let messages = get_local_messages(&sys, "client");
    assume!(messages.len() > 0, "No messages returned by client!")?;
    assume!(messages.len() == 1, "More than one message???")?;
    for m in messages {
        assume!(m.tip == "PONG", "Wrong message type!")?;
        assume!(m.data == r#"{"value": "Hello!"}"#, "Wrong message data!")?;
    }
    Ok(true)
}

fn test_10results_unique(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().borrow_mut().set_delays(0.5, 2.0);
    sys.network().borrow_mut().set_drop_rate(config.drop_rate);
    for i in 0..10 {
        let data = format!(r#"{{"value": "Hello{}!"}}"#, i);
        let ping = Message::new("PING", &data);
        sys.send_local(ping, "client");
        sys.step_until_local_message("client")?;
        let messages = get_local_messages(&sys, "client");
        assume!(messages.len() > 0, "No messages returned by client!")?;
        assume!(messages.len() == 1 + i, "Wrong number of messages!")?;
        assume!(messages[i].tip == "PONG", "Wrong message type!")?;
        assume!(messages[i].data == data, "Wrong message data!")?;
    }
    Ok(true)
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let seed = 1;
    let test: Option<&str> = None;
    init_logger(LevelFilter::Debug);

    let mut tests = TestSuite::new();
    let mut config = TestConfig { seed, drop_rate: 0.0 };
    if test.is_none() || test.unwrap() == "run" {
        tests.add("TEST RUN", test_run, config);
    }
    if test.is_none() || test.unwrap() == "result_reliable" {
        tests.add("TEST RESULT (RELIABLE)", test_result, config);
    }
    if test.is_none() || test.unwrap() == "result_unreliable" {
        config.drop_rate = 0.5;
        tests.add("TEST RESULT (UNRELIABLE)", test_result, config);
    }
    if test.is_none() || test.unwrap() == "10results_unreliable" {
        config.drop_rate = 0.5;
        tests.add("TEST 10 RESULTS (UNRELIABLE)", test_10results, config);
    }
    if test.is_none() || test.unwrap() == "drop_ping" {
        tests.add("TEST RESULT (DROP PING)", test_drop_ping, config);
    }
    if test.is_none() || test.unwrap() == "drop_pong" {
        tests.add("TEST RESULT (DROP PONG)", test_drop_pong, config);
    }
    if test.is_none() || test.unwrap() == "drop_ping2" {
        tests.add("TEST RESULT (DROP PING)", test_drop_ping2, config);
    }
    if test.is_none() || test.unwrap() == "drop_pong2" {
        tests.add("TEST RESULT (DROP PONG)", test_drop_pong2, config);
    }
    if test.is_none() || test.unwrap() == "10results_unique" {
        tests.add("TEST 10 UNIQUE RESULTS (RELIABLE)", test_10results_unique, config);
    }
    if test.is_none() || test.unwrap() == "10results_unique_unreliable" {
        config.drop_rate = 0.5;
        tests.add("TEST 10 UNIQUE RESULTS (UNRELIABLE)", test_10results_unique, config);
    }
    tests.run();
}
