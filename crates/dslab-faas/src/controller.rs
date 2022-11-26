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
use crate::invocation::InvocationRequest;
use crate::invoker::InvocationStatus;
use crate::scheduler::Scheduler;

pub struct Controller {
    function_registry: Rc<RefCell<FunctionRegistry>>,
    hosts: Vec<Rc<RefCell<Host>>>,
    idle_deployer: Box<dyn IdleDeployer>,
    scheduler: Box<dyn Scheduler>,
}

impl Controller {
    pub fn new(
        function_registry: Rc<RefCell<FunctionRegistry>>,
        idle_deployer: Box<dyn IdleDeployer>,
        scheduler: Box<dyn Scheduler>,
    ) -> Self {
        Self {
            function_registry,
            hosts: Vec::new(),
            idle_deployer,
            scheduler,
        }
    }

    fn idle_deploy(&mut self, app_id: u64, time: f64) {
        let reg = self.function_registry.borrow();
        let app = reg.get_app(app_id).unwrap();
        if let Some(host) = self.idle_deployer.deploy(app, &self.hosts) {
            self.hosts[host].borrow_mut().try_deploy(app, time);
        }
    }

    fn invoke(&mut self, request: InvocationRequest, time: f64) -> InvocationStatus {
        let reg = self.function_registry.borrow();
        let app_id = reg.get_function(request.func_id).unwrap().app_id;
        let app = reg.get_app(app_id).unwrap();
        let host = self.scheduler.select_host(app, &self.hosts);
        self.hosts[host].borrow_mut().invoke(request, time)
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
            IdleDeployEvent { id } => {
                self.idle_deploy(id, event.time);
            }
            InvocationStartEvent { request } => {
                self.invoke(request, event.time);
            }
            SimulationEndEvent {} => {
                self.update_end_metrics(event.time);
            }
        });
    }
}
