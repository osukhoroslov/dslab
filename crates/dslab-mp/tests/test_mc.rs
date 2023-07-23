use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::rc::Rc;

use rstest::rstest;
use sugars::{boxed, rc, refcell};

use dslab_mp::context::Context;
use dslab_mp::logger::LogEntry;
use dslab_mp::mc::error::McError;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::state::McState;
use dslab_mp::mc::strategies::bfs::Bfs;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategy::{GoalFn, InvariantFn, PruneFn, StrategyConfig, VisitedStates};
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
            ctx.send(Message::new("COLLECTED", "2"), self.other.clone());
        }
    }

    fn on_local_message(&mut self, _: Message, _: &mut Context) {}

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}

    fn state(&self) -> Rc<dyn ProcessState> {
        rc!(self.cnt.to_string())
    }

    fn set_state(&mut self, state: Rc<dyn ProcessState>) {
        let data = (*state.downcast_rc::<StringProcessState>().unwrap()).clone();
        self.cnt = data.parse::<u64>().unwrap();
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PostponedReceiverNode {
    timer_fired: bool,
    message: Option<Message>,
}

impl PostponedReceiverNode {
    pub fn new() -> Self {
        Self {
            timer_fired: false,
            message: None,
        }
    }
}

impl Process for PostponedReceiverNode {
    fn on_message(&mut self, msg: Message, _: String, ctx: &mut Context) {
        if self.timer_fired {
            ctx.send_local(msg);
        } else {
            self.message = Some(msg);
        }
    }

    fn on_local_message(&mut self, _: Message, ctx: &mut Context) {
        ctx.set_timer("timeout", 1.0);
    }

    fn on_timer(&mut self, _: String, ctx: &mut Context) {
        self.timer_fired = true;
        ctx.send_local(Message::new("TIMER", "timeout"));
        if let Some(msg) = self.message.take() {
            ctx.send_local(msg);
        }
    }

    fn state(&self) -> Rc<dyn ProcessState> {
        rc!(self.clone())
    }

    fn set_state(&mut self, state: Rc<dyn ProcessState>) {
        let postponed_state = (*state).as_any().downcast_ref::<Self>().unwrap();
        self.timer_fired = postponed_state.timer_fired;
        self.message = postponed_state.message.clone();
    }
}

#[derive(Clone)]
struct DumbReceiverNode {}

impl DumbReceiverNode {
    pub fn new() -> Self {
        Self {}
    }
}

impl Process for DumbReceiverNode {
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) {
        ctx.send_local(msg);
    }

    fn on_local_message(&mut self, _: Message, ctx: &mut Context) {
        ctx.set_timer("timeout", 1.0);
    }

    fn on_timer(&mut self, _: String, ctx: &mut Context) {
        ctx.send_local(Message::new("TIMER", "timeout"));
    }
}

#[derive(Clone)]
struct SpammerNode {
    other: String,
    cnt: u64,
}

impl SpammerNode {
    pub fn new(other: &str) -> Self {
        Self {
            other: other.to_string(),
            cnt: 10,
        }
    }
}

impl Process for SpammerNode {
    fn on_message(&mut self, _msg: Message, _from: String, _ctx: &mut Context) {}

    fn on_local_message(&mut self, _msg: Message, ctx: &mut Context) {
        for i in 0..self.cnt {
            ctx.send(Message::new("MESSAGE".to_string(), i.to_string()), self.other.clone());
        }
    }

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}
}

#[derive(Clone, Default)]
struct TimerNode {}

impl Process for TimerNode {
    fn on_message(&mut self, _msg: Message, _from: String, _ctx: &mut Context) {}

    fn on_local_message(&mut self, _msg: Message, ctx: &mut Context) {
        ctx.set_timer("timer", 0.1);
    }

    fn on_timer(&mut self, _timer: String, ctx: &mut Context) {
        ctx.send_local(Message::json("CURRENT_TIME", &ctx.time()))
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

fn build_postponed_delivery_system() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    let process1 = boxed!(PingMessageNode::new("process2"));
    let process2 = boxed!(PostponedReceiverNode::new());
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys
}

fn build_dumb_delivery_system_with_useless_timer() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    let process1 = boxed!(PingMessageNode::new("process2"));
    let process2 = boxed!(DumbReceiverNode::new());
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys
}

fn build_spammer_delivery_system() -> System {
    let mut sys = System::new(12345);
    sys.add_node("node1");
    sys.add_node("node2");
    let process1 = boxed!(SpammerNode::new("process2"));
    let process2 = boxed!(PingMessageNode::new("process1"));
    sys.add_process("process1", process1, "node1");
    sys.add_process("process2", process2, "node2");
    sys
}

fn build_timer_system(clock_skew: f64) -> System {
    let mut sys = System::new(12345);
    sys.add_node("node");
    sys.set_node_clock_skew("node", clock_skew);
    let process = boxed!(TimerNode::default());
    sys.add_process("process", process, "node");
    sys
}

fn build_strategy_config(prune: PruneFn, goal: GoalFn, invariant: InvariantFn) -> StrategyConfig {
    StrategyConfig::default().prune(prune).goal(goal).invariant(invariant)
}

fn build_mc_from_config(sys: &System, strategy_name: String, config: StrategyConfig) -> ModelChecker {
    if strategy_name == "bfs" {
        ModelChecker::new::<Bfs>(sys, config)
    } else {
        ModelChecker::new::<Dfs>(sys, config)
    }
}

fn build_mc(sys: &System, strategy_name: String, prune: PruneFn, goal: GoalFn, invariant: InvariantFn) -> ModelChecker {
    let config = build_strategy_config(prune, goal, invariant);
    build_mc_from_config(sys, strategy_name, config)
}

fn build_dumb_counter_invariant(count_states: Rc<RefCell<i32>>) -> InvariantFn {
    boxed!(move |_: &McState| {
        *count_states.borrow_mut() += 1;
        Ok(())
    })
}

fn build_n_messages_goal(node: String, process: String, n: usize) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.node_states[&node][&process].local_outbox.len() == n {
            Some("final".to_string())
        } else {
            None
        }
    })
}

fn build_one_message_get_data_goal(node: String, proc: String, goal_data: Rc<RefCell<Vec<String>>>) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.node_states[&node][&proc].local_outbox.len() == 1 {
            (*goal_data.borrow_mut()).push(state.node_states[&node][&proc].local_outbox[0].data.clone());
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

fn build_reached_depth_goal(depth: u64) -> GoalFn {
    boxed!(move |state: &McState| {
        if state.events.available_events_num() == 0 && state.depth >= depth {
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

fn two_nodes_started_trace() -> Vec<LogEntry> {
    vec![
        LogEntry::NodeStarted {
            time: 0.0,
            node: "node1".to_string(),
            node_id: 1,
        },
        LogEntry::NodeStarted {
            time: 0.0,
            node: "node2".to_string(),
            node_id: 2,
        },
        LogEntry::ProcessStarted {
            time: 0.0,
            node: "node1".to_string(),
            proc: "process1".to_string(),
        },
        LogEntry::ProcessStarted {
            time: 0.0,
            node: "node2".to_string(),
            proc: "process2".to_string(),
        },
    ]
}

fn one_message_sent_before_mc_trace(msg: Message) -> Vec<LogEntry> {
    let mut trace = two_nodes_started_trace();
    trace.extend(vec![
        LogEntry::LocalMessageReceived {
            time: 0.0,
            msg_id: "node1-process1-0".to_string(),
            node: "node1".to_string(),
            proc: "process1".to_string(),
            msg: msg.clone(),
        },
        LogEntry::MessageSent {
            time: 0.0,
            msg_id: "0".to_string(),
            src_node: "node1".to_string(),
            src_proc: "process1".to_string(),
            dest_node: "node2".to_string(),
            dest_proc: "process2".to_string(),
            msg,
        },
    ]);
    trace
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_state_ok(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = boxed!(|_: &McState| Some("final".to_string()));

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let mut mc = build_mc(&build_ping_system(), strategy_name, prune, goal, invariant);
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

    let mut mc = build_mc(&build_ping_system(), strategy_name, prune, goal, invariant);
    let result = mc.run();
    assert!(if let Err(err) = result {
        err.message() == "broken"
    } else {
        false
    });
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_state_no_goal(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = boxed!(|_: &McState| None);
    let invariant = boxed!(|_: &McState| Ok(()));

    let mut mc = build_mc(&build_ping_system(), strategy_name, prune, goal, invariant);
    let result = mc.run();

    let mut expected_trace = two_nodes_started_trace();
    expected_trace.push(LogEntry::McStarted {});
    let expected = Err(McError::new(
        "nothing left to do to reach the goal".to_string(),
        expected_trace,
    ));
    assert_eq!(result, expected);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn two_states_one_message_ok(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_n_messages_goal("node2".to_string(), "process2".to_string(), 1);

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "some_data"));
    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);

    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 2);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn two_states_one_message_pruned(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| Some("pruned".to_string()));

    let goal = boxed!(|_: &McState| None);

    let count_states = rc!(refcell!(0));
    let invariant = build_dumb_counter_invariant(count_states.clone());

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "some_data"));

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
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

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "some_data"));
    sys.network().set_drop_rate(0.5);

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();

    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 3);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_dropped_with_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);
    let goal = build_n_messages_goal("node2".to_string(), "process2".to_string(), 1);
    let invariant = boxed!(|_: &McState| Ok(()));

    let mut sys = build_ping_system();
    let msg = Message::new("PING", "some_data");
    sys.send_local_message("process1", msg.clone());
    sys.network().set_drop_rate(0.5);

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();

    let mut expected_trace = one_message_sent_before_mc_trace(msg.clone());
    expected_trace.extend(vec![
        LogEntry::McStarted {},
        LogEntry::McMessageDropped {
            msg,
            src: "process1".to_string(),
            dest: "process2".to_string(),
        },
    ]);

    let expected = Err(McError::new(
        "nothing left to do to reach the goal".to_string(),
        expected_trace,
    ));
    assert_eq!(result, expected);
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

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "some_data"));
    sys.network().set_dupl_rate(0.5);

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();

    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 9);
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

    let mut sys = build_ping_system();
    let msg = Message::new("PING", "some_data");
    let src = "process1".to_string();
    let dest = "process2".to_string();
    sys.send_local_message("process1", msg.clone());
    sys.network().set_dupl_rate(0.5);

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();

    let mut expected_trace = one_message_sent_before_mc_trace(msg.clone());
    let expected_message_duplicated_event = LogEntry::McMessageDuplicated {
        msg: msg.clone(),
        src: src.clone(),
        dest: dest.clone(),
    };
    let expected_message_received_event = LogEntry::McMessageReceived {
        msg: msg.clone(),
        src,
        dest: dest.clone(),
    };
    let expected_local_message_sent_event = LogEntry::McLocalMessageSent { msg, proc: dest };
    expected_trace.extend(vec![
        LogEntry::McStarted {},
        expected_message_duplicated_event,
        expected_message_received_event.clone(),
        expected_local_message_sent_event.clone(),
        expected_message_received_event,
        expected_local_message_sent_event,
    ]);
    let expected = Err(McError::new("too many messages".to_string(), expected_trace));
    assert_eq!(result, expected);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn one_message_corrupted_without_guarantees(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal_data = rc!(refcell!(vec![]));
    let goal = build_one_message_get_data_goal("node2".to_string(), "process2".to_string(), goal_data.clone());

    let invariant = boxed!(|_: &McState| Ok(()));

    let mut sys = build_ping_system();
    sys.send_local_message("process1", Message::new("PING", "{\"key1\": \"value1\", \"key2\": 33}"));
    sys.network().set_corrupt_rate(0.5);

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
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

    let mut sys = build_ping_system_with_collector();
    sys.send_local_message("process1", Message::new("PING", "some_data_1"));
    sys.send_local_message("process1", Message::new("PING", "some_data_2"));

    let config = build_strategy_config(prune, goal, invariant).visited_states(VisitedStates::Full(HashSet::default()));
    let mut mc = build_mc_from_config(&sys, strategy_name, config);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 5);
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn timer(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_no_events_left_goal();

    let count_states = rc!(refcell!(0));
    let count_states_cloned = count_states.clone();
    let invariant = boxed!(move |state: &McState| {
        *count_states_cloned.borrow_mut() += 1;
        let proc2_outbox = &state.node_states["node2"]["process2"].local_outbox;

        if !proc2_outbox.is_empty() && proc2_outbox[0].tip != "TIMER" {
            return Err("invalid order".to_string());
        }

        if state.events.available_events_num() == 0 {
            if proc2_outbox.len() == 2 && proc2_outbox[1].tip == "PING" {
                return Ok(());
            } else {
                return Err("wrong set of delivered events".to_string());
            }
        }

        Ok(())
    });

    let mut sys = build_postponed_delivery_system();
    sys.send_local_message("process1", Message::new("PING", "some_data_1"));
    sys.send_local_message("process2", Message::new("WAKEUP", "start_timer"));

    let config = build_strategy_config(prune, goal, invariant).visited_states(VisitedStates::Full(HashSet::default()));
    let mut mc = build_mc_from_config(&sys, strategy_name, config);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(*count_states.borrow(), 4); // final states for both branches are equal: first timer, then message
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn useless_timer(#[case] strategy_name: String) {
    let prune = boxed!(|_: &McState| None);

    let goal = build_no_events_left_goal();

    let invariant = boxed!(move |state: &McState| {
        let proc2_outbox = &state.node_states["node2"]["process2"].local_outbox;

        if state.events.available_events_num() == 0 {
            if !proc2_outbox.is_empty() && proc2_outbox[0].tip != "TIMER" {
                return Err("invalid order".to_string());
            }
            if proc2_outbox.len() == 2 && proc2_outbox[1].tip == "PING" {
                return Ok(());
            } else {
                return Err("wrong set of delivered events".to_string());
            }
        }

        Ok(())
    });

    let mut sys = build_dumb_delivery_system_with_useless_timer();
    sys.send_local_message("process1", Message::new("PING", "some_data_1"));
    sys.send_local_message("process2", Message::new("WAKEUP", "start_timer"));

    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();
    assert!(result.is_err());

    let err = result.err().unwrap();
    assert_eq!(err.message(), "invalid order");

    let trace = err.trace();
    assert_eq!(trace.len(), 13);
    assert!(matches!(trace[9].clone(), LogEntry::McMessageReceived { .. }));
    assert!(matches!(trace[11].clone(), LogEntry::McTimerFired { .. }));
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn many_dropped_messages(#[case] strategy_name: String) {
    let invariant = boxed!(|state: &McState| {
        if state.events.available_events_num() > 1 {
            Err("MessageDropped directives should have strict order".to_owned())
        } else {
            Ok(())
        }
    });
    let prune = boxed!(|_: &McState| None);
    let goal = build_reached_depth_goal(10);
    let mut sys = build_spammer_delivery_system();
    sys.send_local_message("process1", Message::new("START", "start spamming!!!"));

    sys.network().drop_outgoing("node1");
    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);
    let result = mc.run();
    assert!(result.is_ok());
}

#[rstest]
#[case(0.0)]
#[case(0.1)]
fn context_time(#[case] clock_skew: f64) {
    let prune = boxed!(|_: &McState| None);
    let goal_data = rc!(refcell!(vec![]));
    let goal = build_one_message_get_data_goal("node".to_string(), "process".to_string(), goal_data.clone());
    let invariant = boxed!(|_: &McState| Ok(()));

    let mut sys = build_timer_system(clock_skew);
    sys.send_local_message("process", Message::new("PING", "some_data"));
    let mut mc = build_mc(&sys, "dfs".to_string(), prune, goal, invariant);
    let result = mc.run();
    assert!(result.is_ok());
    assert_eq!(goal_data.borrow().len(), 1);
    assert_eq!(
        str::parse::<f64>(&goal_data.borrow()[0]).unwrap(),
        0.1 + clock_skew,
        "expected timestamp formula: 0.1 * depth + clock_skew"
    );
}

#[rstest]
#[case("dfs")]
#[case("bfs")]
fn collect_mode(#[case] strategy_name: String) {
    let mut sys: System = build_postponed_delivery_system();
    sys.send_local_message("process2", Message::new("WAKEUP", ""));

    // first stage: either process wakes or it doesn't: collect both states
    let invariant = boxed!(|_: &McState| Ok(()));
    let prune = boxed!(|_: &McState| None);
    let goal = build_reached_depth_goal(1);
    let collect = boxed!(|_: &McState| true);
    let config = build_strategy_config(prune, goal, invariant).collect(collect);
    let mut mc = build_mc_from_config(&sys, strategy_name.clone(), config);
    let run_stats = mc.run().expect("run failed but shouldn't");
    let states = run_stats.collected_states;
    assert_eq!(states.len(), 2);

    // second stage: deliver a message from client
    // result is 2 local messages: 1 after timer wakeup and 1 after receiving message
    let invariant = boxed!(|_: &McState| Ok(()));
    let prune = boxed!(|_: &McState| None);
    let goal = build_n_messages_goal("node2".to_string(), "process2".to_string(), 2);
    let mut mc = build_mc(&sys, strategy_name, prune, goal, invariant);

    let res = mc.run_from_states_with_change(states, |sys| {
        sys.send_local_message(
            "node1".to_string(),
            "process1".to_string(),
            Message::new("PING", "some_data_1"),
        );
    });
    assert!(res.is_ok());
}
