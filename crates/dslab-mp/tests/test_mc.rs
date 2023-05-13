use std::cell::RefCell;
use std::rc::Rc;

use rstest::rstest;
use sugars::{boxed, rc, refcell};

use dslab_mp::context::Context;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::state::McState;
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategy::{ExecutionMode, GoalFn, InvariantFn, PruneFn, Strategy};
use dslab_mp::message::Message;
use dslab_mp::process::{Process, ProcessState, StringProcessState};
use dslab_mp::system::System;

#[derive(Clone)]
struct PingMessageNode {
    other: String,
}

impl PingMessageNode {
    pub fn new(other: &str) -> Self {
        Self {
            other: other.to_string(),
        }
    }
}

impl Process for PingMessageNode {
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) {
        ctx.send_local(msg);
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) {
        ctx.send(msg, self.other.clone());
    }

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}
}

#[derive(Clone)]
struct CollectorNode {
    other: String,
    cnt: u64,
}

impl CollectorNode {
    pub fn new(other: &str) -> Self {
        Self {
            other: other.to_string(),
            cnt: 0,
        }
    }
}

impl Process for CollectorNode {
    fn on_message(&mut self, _msg: Message, _from: String, ctx: &mut Context) {
        self.cnt += 1;
        if self.cnt == 2 {
            ctx.send(
                Message {
                    tip: "COLLECTED".to_string(),
                    data: 2.to_string(),
                },
                self.other.clone(),
            );
        }
    }

    fn on_local_message(&mut self, _: Message, _: &mut Context) {}

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}

    fn state(&self) -> Box<dyn ProcessState> {
        boxed!(self.cnt.to_string())
    }

    fn set_state(&mut self, state: Box<dyn ProcessState>) {
        let data = *state.downcast::<StringProcessState>().unwrap();
        self.cnt = data.parse::<u64>().unwrap();
    }
}

fn build_ping_system() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    let process1 = boxed!(PingMessageNode::new("process2"));
    let process2 = boxed!(PingMessageNode::new("process1"));
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys
}

fn build_ping_system_with_collector() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    sys.add_node("node3");
    let process1 = boxed!(PingMessageNode::new("process2"));
    let process2 = boxed!(CollectorNode::new("process3"));
    let process3 = boxed!(PingMessageNode::new("process2"));
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys.add_process("process3", process3, "node3");
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

fn build_one_message_get_data_goal(goal_data: Rc<RefCell<Vec<String>>>) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.node_states["node2"]["process2"].local_outbox.len() == 1 {
            (*goal_data.borrow_mut()).push(state.node_states["node2"]["process2"].local_outbox[0].data.clone());
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

fn build_no_events_left_with_counter_goal(count_goal_states: Rc<RefCell<i32>>) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.events.available_events_num() == 0 {
            *count_goal_states.borrow_mut() += 1;
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
    let mut mc = ModelChecker::new(&build_ping_system(), strategy);
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
    let mut mc = ModelChecker::new(&build_ping_system(), strategy);
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
    let mut mc = ModelChecker::new(&build_ping_system(), strategy);
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

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "some_data"));

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

    let mut sys = build_ping_system();
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

    let mut sys = build_ping_system();
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

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_dropped_with_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = build_one_message_goal();
    let invariant = boxed!(|_: &McState| Ok(()));

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_ping_system();
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

    assert!(if let Err(msg) = result {
        msg == "nothing left to do to reach the goal"
    } else {
        false
    });
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_duplicated_without_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let count_goal_states = rc!(refcell!(0));
    let goal = build_no_events_left_with_counter_goal(count_goal_states.clone());

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_ping_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data".to_string(),
        },
    );
    sys.network().set_dupl_rate(0.5);

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();

    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 6);
    assert_eq!(*count_goal_states.borrow(), 3);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_duplicated_with_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_no_events_left_goal();

    let invariant = boxed!(|state: &McState| {
        if state.node_states["node2"]["process2"].local_outbox.len() > 1 {
            Err("too many messages".to_string())
        } else {
            Ok(())
        }
    });

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_ping_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data".to_string(),
        },
    );
    sys.network().set_dupl_rate(0.5);

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();

    assert!(if let Err(msg) = result {
        msg == "too many messages"
    } else {
        false
    });
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_corrupted_without_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal_data = rc!(refcell!(vec![]));
    let goal = build_one_message_get_data_goal(goal_data.clone());

    let invariant = boxed!(|_: &McState| Ok(()));

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_ping_system();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some text".to_string(),
        },
    );
    sys.network().set_corrupt_rate(0.5);

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();

    assert!(result.is_ok());
    assert_eq!(goal_data.borrow().len(), 2);
    assert_ne!(goal_data.borrow()[0], goal_data.borrow()[1]);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn visited_states(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_no_events_left_goal();

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let strategy = create_strategy(strategy_name, prune, goal, invariant, ExecutionMode::Default);

    let mut sys = build_ping_system_with_collector();
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data_1".to_string(),
        },
    );
    sys.send_local_message(
        "process1",
        Message {
            tip: "PING".to_string(),
            data: "some_data_2".to_string(),
        },
    );

    let mut mc = ModelChecker::new(&sys, strategy);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 5);
}
