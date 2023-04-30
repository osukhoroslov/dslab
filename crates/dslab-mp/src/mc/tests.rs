use std::cell::RefCell;
use std::rc::Rc;
use rstest::rstest;
use sugars::{boxed, rc, refcell};

use crate::context::Context;
use crate::mc::model_checker::ModelChecker;
use crate::mc::strategies::bfs::Bfs;
use crate::mc::strategies::dfs::Dfs;
use crate::mc::strategy::{ExecutionMode, GoalFn, InvariantFn, PruneFn, Strategy};
use crate::mc::system::McState;
use crate::message::Message;
use crate::process::Process;
use crate::system::System;

#[derive(Clone)]
struct TestNode {
    other: String,
}

impl TestNode {
    pub fn new(other: &str) -> Self {
        Self {
            other: other.to_string(),
        }
    }
}

impl Process for TestNode {
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) {
        ctx.send_local(msg);
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) {
        ctx.send(msg, self.other.clone());
    }

    fn on_timer(&mut self, timer: String, ctx: &mut Context) {
        ctx.send_local(Message {
            tip: "timer".to_string(),
            data: timer,
        });
    }
}

fn build_system() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    let process1 = boxed!(TestNode::new("process2"));
    let process2 = boxed!(TestNode::new("process1"));
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys
}

fn create_strategy(
    strategy_name: String,
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    execution_mode: ExecutionMode,
) -> Box<dyn Strategy> {
    if strategy_name == "bfs" {
        boxed!(Bfs::new(prune, goal, invariant, execution_mode))
    } else {
        boxed!(Dfs::new(prune, goal, invariant, execution_mode))
    }
}

fn build_dumb_counter_invariant(count_states: Rc<RefCell<i32>>) -> InvariantFn {
    boxed!(move |_: &McState| {
        *count_states.borrow_mut() += 1;
        Ok(())
    })
}

fn build_one_message_goal() -> GoalFn {
    boxed!(|state: &McState| {
        if state.node_states["node2"]["process2"].local_outbox.len() == 1 {
            Some("final".to_string())
        } else {
            None
        }
    })
}

fn build_no_events_left_goal() -> GoalFn {
    boxed!(|state: &McState| {
        if state.events.available_events_num() == 0 {
            Some("final".to_string())
        } else {
            None
        }
    })
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_state_ok(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = boxed!(|_: &McState| Some("final".to_string()));

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);
    let mut mc = ModelChecker::new(&build_system(), strategy);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 1);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_state_broken_invariant(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = boxed!(|_: &McState| Some("final".to_string()));
    let invariant = boxed!(|_: &McState| Err("broken".to_string()));

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);
    let mut mc = ModelChecker::new(&build_system(), strategy);
    let result = mc.run();
    assert!(if let Err(msg) = result { msg == "broken" } else { false });
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_state_no_goal(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = boxed!(|_: &McState| None);
    let invariant = boxed!(|_: &McState| Ok(()));

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);
    let mut mc = ModelChecker::new(&build_system(), strategy);
    let result = mc.run();
    assert!(if let Err(msg) = result {
        msg == "nothing left to do to reach the goal"
    } else {
        false
    });
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn two_states_one_message_ok(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_one_message_goal();

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data".to_string(),
        },
    );

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 2);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn two_states_one_message_pruned(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| Some("pruned".to_string()));

    let goal = build_one_message_goal();

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data".to_string(),
        },
    );

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 1);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_dropped_without_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_no_events_left_goal();

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data".to_string(),
        },
    );
    sys.network().set_drop_rate(0.5);

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();

    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 3);
}
