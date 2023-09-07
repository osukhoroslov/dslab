use rstest::rstest;
use std::collections::HashSet;
use std::env;
use sugars::boxed;

use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::predicates::{goals, invariants, prunes};
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategy::StrategyConfig;
use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp_python::PyProcessFactory;

macro_rules! run_mc {
    ($sys:expr, $config:expr, $strategy:ident) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run::<Bfs>($config),
            "dfs" => ModelChecker::new(&$sys).run::<Dfs>($config),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
    ($sys:expr, $config:expr, $strategy:ident, $callback:expr) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run_with_change::<Bfs>($config, $callback),
            "dfs" => ModelChecker::new(&$sys).run_with_change::<Dfs>($config, $callback),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
    ($sys:expr, $config:expr, $strategy:ident, $states:ident, $callback:expr) => {
        match $strategy {
            "bfs" => ModelChecker::new(&$sys).run_from_states_with_change::<Bfs>($config, $states, $callback),
            "dfs" => ModelChecker::new(&$sys).run_from_states_with_change::<Dfs>($config, $states, $callback),
            s => panic!("Unknown strategy name: {}", s),
        }
    };
}

fn build_system() -> System {
    let mut sys = System::new(12345);
    sys.add_node("server-node");
    sys.add_node("client-node");

    let py_path = [env::current_dir().unwrap().to_str().unwrap(), "/python"].join("");
    env::set_var("PYTHONPATH", py_path);
    let impl_path = env::var("PYTHONPATH").unwrap() + "/../tests/python/retry_runtime_error.py";

    let server_f = PyProcessFactory::new(impl_path.as_str(), "PingServer");
    let server = boxed!(server_f.build(("server",), 12345));
    let client_f = PyProcessFactory::new(impl_path.as_str(), "PingClient");
    let client = boxed!(client_f.build(("client", "server"), 12345));

    sys.add_process("server", server, "server-node");
    sys.add_process("client", client, "client-node");
    sys
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn python_runtime_error(#[case] strategy_name: &str) {
    let system = build_system();
    let data = r#"{"value": 0}"#.to_string();
    let messages_expected = HashSet::<String>::from_iter([data.clone()]);

    let strategy_config = StrategyConfig::default()
        .prune(prunes::sent_messages_limit(4))
        .goal(goals::got_n_local_messages("client-node", "client", 1))
        .invariant(invariants::all_invariants(vec![
            invariants::received_messages("client-node", "client", messages_expected),
            invariants::state_depth(20),
        ]));

    let res = run_mc!(&system, strategy_config, strategy_name, |system| {
        system
            .send_local_message("client-node", "client", Message::new("PING", &data))
            .unwrap();
        system
            .send_local_message("client-node", "client", Message::new("PING", &data))
            .unwrap();
    });

    assert!(res.is_err());
    assert_eq!(
        res.err().unwrap().message(),
        "TypeError: unsupported operand type(s) for +=: 'NoneType' and 'int'"
    );
}
