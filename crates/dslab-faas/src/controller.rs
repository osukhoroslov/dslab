//! Platform controller.

use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::deployer::IdleDeployer;
use crate::event::{IdleDeployEvent, InvocationStartEvent, SimulationEndEvent};
use crate::function::FunctionRegistry;
use crate::host::Host;
use crate::invoker::InvokerDecision;
use crate::scheduler::Scheduler;
use crate::stats::Stats;

/// Responsible for managing incoming invocation requests and deploying prewarmed containers.
pub struct Controller {
    function_registry: Rc<RefCell<FunctionRegistry>>,
    hosts: Vec<Rc<RefCell<Host>>>,
    idle_deployer: Box<dyn IdleDeployer>,
    scheduler: Box<dyn Scheduler>,
    stats: Rc<RefCell<Stats>>,
}

impl Controller {
    pub fn new(
        function_registry: Rc<RefCell<FunctionRegistry>>,
        idle_deployer: Box<dyn IdleDeployer>,
        scheduler: Box<dyn Scheduler>,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        Self {
            function_registry,
            hosts: Vec::new(),
            idle_deployer,
            scheduler,
            stats,
        }
    }

    fn idle_deploy(&mut self, app_id: usize, expected_invocation: u64, time: f64) {
        let next_invocation = self.stats.borrow().app_stats.get(app_id).unwrap().invocations;
        if next_invocation != expected_invocation {
            // do not deploy container when the expected invocation has already happened
            return;
        }
        let reg = self.function_registry.borrow();
        let app = reg.get_app(app_id).unwrap();
        if let Some(host) = self.idle_deployer.deploy(app, &self.hosts) {
            self.hosts[host].borrow_mut().try_deploy(app, time);
        }
    }

    fn invoke(&mut self, id: usize, func_id: usize, time: f64) -> InvokerDecision {
        let reg = self.function_registry.borrow();
        let app = reg.get_app_by_function(func_id).unwrap();
        let host = self.scheduler.select_host(app, &self.hosts);
        self.hosts[host].borrow_mut().invoke(id, time)
    }

    pub fn add_host(&mut self, host: Rc<RefCell<Host>>) {
        self.hosts.push(host);
    }

    fn update_end_metrics(&mut self, time: f64) {
        for host in &mut self.hosts {
            host.borrow_mut().update_end_metrics(time);
        }
    }
}

impl EventHandler for Controller {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            IdleDeployEvent {
                id,
                expected_invocation,
            } => {
                self.idle_deploy(id, expected_invocation, event.time);
            }
            InvocationStartEvent { id, func_id } => {
                self.invoke(id, func_id, event.time);
            }
            SimulationEndEvent {} => {
                self.update_end_metrics(event.time);
            }
        });
    }
}
