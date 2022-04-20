use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use simcore::cast;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::simulation::Simulation;

use crate::coldstart::ColdStartPolicy;
use crate::deployer::IdleDeployer;
use crate::event::{IdleDeployEvent, InvocationStartEvent, SimulationEndEvent};
use crate::function::FunctionRegistry;
use crate::invocation::{InvocationRegistry, InvocationRequest};
use crate::invoker::{InvocationStatus, Invoker, InvokerLogic};
use crate::resource::ResourceProvider;
use crate::scheduler::Scheduler;
use crate::simulation::HandlerId;
use crate::stats::Stats;

pub struct Controller {
    coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    function_registry: Rc<RefCell<FunctionRegistry>>,
    idle_deployer: Box<dyn IdleDeployer>,
    invocation_registry: Rc<RefCell<InvocationRegistry>>,
    invokers: Vec<Rc<RefCell<Invoker>>>,
    scheduler: Box<dyn Scheduler>,
    stats: Rc<RefCell<Stats>>,
}

impl Controller {
    pub fn new(
        coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
        function_registry: Rc<RefCell<FunctionRegistry>>,
        idle_deployer: Box<dyn IdleDeployer>,
        invocation_registry: Rc<RefCell<InvocationRegistry>>,
        scheduler: Box<dyn Scheduler>,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        Self {
            coldstart,
            idle_deployer,
            function_registry,
            invocation_registry,
            invokers: Vec::new(),
            scheduler,
            stats,
        }
    }

    fn idle_deploy(&mut self, app_id: u64, time: f64) {
        let reg = self.function_registry.borrow();
        let app = reg.get_app(app_id).unwrap();
        if let Some(invoker) = self.idle_deployer.deploy(app, &mut self.invokers) {
            self.invokers[invoker].borrow_mut().try_deploy(app, time);
        }
    }

    fn invoke(&mut self, request: InvocationRequest, time: f64) -> InvocationStatus {
        let reg = self.function_registry.borrow();
        let app_id = reg.get_function(request.id).unwrap().app_id;
        let app = reg.get_app(app_id).unwrap();
        let invoker = self.scheduler.select_invoker(app, &self.invokers);
        self.invokers[invoker].borrow_mut().invoke(request, time)
    }

    pub fn new_invoker(
        &mut self,
        controller_id: HandlerId,
        logic: Box<dyn InvokerLogic>,
        resources: ResourceProvider,
        sim: &mut Simulation,
    ) -> u64 {
        let id = self.invokers.len() as u64;
        let invoker = Rc::new(RefCell::new(Invoker::new(
            id,
            self.coldstart.clone(),
            controller_id,
            self.function_registry.clone(),
            self.invocation_registry.clone(),
            logic,
            resources,
            sim,
            self.stats.clone(),
        )));
        let handler_id = sim.add_handler(format!("invoker_{}", id), invoker.clone());
        invoker.borrow_mut().setup_handler(handler_id);
        self.invokers.push(invoker);
        id
    }

    fn update_end_metrics(&mut self, time: f64) {
        for invoker in &mut self.invokers {
            invoker.borrow_mut().update_end_metrics(time);
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
