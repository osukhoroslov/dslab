//! Model checker configuration and launching.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use sugars::boxed;

use dslab_core::cast;

use crate::events::{MessageReceived, TimerFired};
use crate::logger::LogEntry;
use crate::mc::events::McEvent;
use crate::mc::network::{DeliveryOptions, McNetwork};
use crate::mc::node::McNode;
use crate::mc::pending_events::PendingEvents;
use crate::mc::state::McState;
use crate::mc::strategy::{McResult, McStats, Strategy, StrategyConfig};
use crate::mc::system::{McSystem, McTime};
use crate::mc::trace_handler::TraceHandler;
use crate::system::System;

/// Main class of (and entrypoint to) the model checking testing technique.
pub struct ModelChecker {
    system: McSystem,
    strategy: Box<dyn Strategy>,
}

impl ModelChecker {
    /// Creates a new model checker with the specified strategy
    /// and initial state equal to the current state of the system.
    pub fn new<S: Strategy + 'static>(sys: &System, strategy_config: StrategyConfig) -> Self {
        // Setup strategy which specifies rules for state exploration
        let strategy = boxed!(S::build(strategy_config));

        // Setup environment for model checker
        let sim = sys.sim();

        let mc_net = Rc::new(RefCell::new(McNetwork::new(sys.network())));

        let trace = sys.logger().trace().clone();
        let trace_handler = Rc::new(RefCell::new(TraceHandler::new(trace)));

        let mut nodes: HashMap<String, McNode> = HashMap::new();
        for node in sys.nodes() {
            let node = sys.get_node(&node).unwrap();
            nodes.insert(
                node.name.clone(),
                McNode::new(
                    node.processes(),
                    mc_net.clone(),
                    trace_handler.clone(),
                    node.clock_skew(),
                ),
            );
        }

        let mut events = PendingEvents::new();
        for event in sim.dump_events() {
            cast!(match event.data {
                MessageReceived { msg, src, dst, .. } => {
                    events.push(McEvent::MessageReceived {
                        msg,
                        src,
                        dst,
                        options: DeliveryOptions::NoFailures(McTime::from(mc_net.borrow().max_delay())),
                    });
                }
                TimerFired { proc, timer } => {
                    events.push(McEvent::TimerFired {
                        proc,
                        timer,
                        timer_delay: McTime::from(0.0),
                    });
                }
            });
        }

        Self {
            system: McSystem::new(nodes, mc_net, events, trace_handler),
            strategy,
        }
    }

    fn run_impl(&mut self) -> McResult {
        self.strategy.mark_visited(self.system.get_state());
        self.strategy.run(&mut self.system)
    }

    /// Runs model checking and returns the result on completion.
    pub fn run(&mut self) -> McResult {
        self.system.trace_handler.borrow_mut().push(LogEntry::McStarted {});
        self.run_impl()
    }

    /// Runs model checking after applying callback.
    pub fn run_with_change<F>(&mut self, preliminary_callback: F) -> McResult
    where
        F: FnOnce(&mut McSystem),
    {
        self.system.trace_handler.borrow_mut().push(LogEntry::McStarted {});
        preliminary_callback(&mut self.system);
        self.run_impl()

    }

    /// Runs model checking from a set of initial states.
    pub fn run_from_states(&mut self, states: HashSet<McState>) -> McResult {
        self.run_from_states_with_change(states, |_| {})
    }

    /// Runs model checking from a set of initial states after applying callback.
    pub fn run_from_states_with_change<F>(&mut self, states: HashSet<McState>, preliminary_callback: F) -> McResult
    where
        F: Fn(&mut McSystem),
    {
        let mut total_stats = McStats::default();
        let mut states = Vec::from_iter(states);
        // sort starting states by increasing depth to produce shorter error traces
        states.sort_by_key(|x| x.depth);
        for state in states {
            self.system.set_state(state);
            self.system.trace_handler.borrow_mut().push(LogEntry::McStarted {});
            preliminary_callback(&mut self.system);
            let stats = self.run_impl()?;
            total_stats.combine(stats);
        }
        Ok(total_stats)
    }
}
