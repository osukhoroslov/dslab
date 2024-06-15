//! Model checker configuration and launching.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

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
}

impl ModelChecker {
    /// Creates a new model checker with the specified strategy
    /// and initial state equal to the current state of the system.
    pub fn new(sys: &System) -> Self {
        // Setup environment for model checker
        let sim = sys.sim();

        let mc_net = McNetwork::new(sys.network());

        let trace = sys.logger().trace().clone();
        let trace_handler = Rc::new(RefCell::new(TraceHandler::new(trace)));

        let mut nodes: HashMap<String, McNode> = HashMap::new();
        for node in sys.nodes() {
            let node = sys.get_node(&node).unwrap();
            nodes.insert(
                node.name.clone(),
                McNode::new(
                    node.name.clone(),
                    node.processes(),
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
                        options: DeliveryOptions::NoFailures(McTime::from(mc_net.max_delay())),
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
        }
    }

    fn run_impl<S>(&mut self, strategy: &mut S, preliminary_callback: impl FnOnce(&mut McSystem)) -> McResult
    where
        S: Strategy,
    {
        let initial_state = self.system.get_state();
        self.system.trace_handler.borrow_mut().push(LogEntry::McStarted {});
        preliminary_callback(&mut self.system);
        strategy.mark_visited(self.system.get_state());
        let res = strategy.run(&mut self.system);
        strategy.reset();
        // McSystem is always rolled back to the state before MC run
        self.system.set_state(initial_state);
        res
    }

    /// Runs model checking and returns the result on completion.
    pub fn run<S: Strategy>(&mut self, strategy_config: StrategyConfig) -> McResult {
        self.run_with_change::<S>(strategy_config, |_| {})
    }

    /// Runs model checking after applying callback.
    pub fn run_with_change<S>(
        &mut self,
        strategy_config: StrategyConfig,
        preliminary_callback: impl FnOnce(&mut McSystem),
    ) -> McResult
    where
        S: Strategy,
    {
        let mut strategy = S::build(strategy_config);
        self.run_impl(&mut strategy, preliminary_callback)
    }

    /// Runs model checking from a set of initial states.
    pub fn run_from_states<S: Strategy>(
        &mut self,
        strategy_config: StrategyConfig,
        states: HashSet<McState>,
    ) -> McResult {
        self.run_from_states_with_change::<S>(strategy_config, states, |_| {})
    }

    /// Runs model checking from a set of initial states after applying callback.
    pub fn run_from_states_with_change<S>(
        &mut self,
        strategy_config: StrategyConfig,
        states: HashSet<McState>,
        preliminary_callback: impl Fn(&mut McSystem),
    ) -> McResult
    where
        S: Strategy,
    {
        let mut total_stats = McStats::default();
        let mut states = Vec::from_iter(states);
        let mut strategy = S::build(strategy_config);

        // sort starting states by increasing depth to produce shorter error traces
        states.sort_by_key(|x| x.depth);
        for state in states {
            self.system.set_state(state);
            let stats = self.run_impl(&mut strategy, &preliminary_callback)?;
            total_stats.combine(stats);
        }
        Ok(total_stats)
    }
}
