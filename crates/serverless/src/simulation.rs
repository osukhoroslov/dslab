use std::cell::RefCell;
use std::rc::Rc;

use simcore::context::SimulationContext;
use simcore::simulation::Simulation;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::controller::Controller;
use crate::deployer::{BasicDeployer, IdleDeployer};
use crate::event::{InvocationStartEvent, SimulationEndEvent};
use crate::function::{Function, FunctionRegistry, Group};
use crate::invocation::{InvocationRegistry, InvocationRequest};
use crate::invoker::{BasicInvoker, InvokerLogic};
use crate::resource::{Resource, ResourceNameResolver, ResourceProvider, ResourceRequirement};
use crate::scheduler::{BasicScheduler, Scheduler};
use crate::stats::Stats;

pub type HandlerId = simcore::component::Id;

pub struct ServerlessSimulation {
    controller: Rc<RefCell<Controller>>,
    controller_handler_id: HandlerId,
    function_registry: Rc<RefCell<FunctionRegistry>>,
    ctx: SimulationContext,
    resource_name_resolver: ResourceNameResolver,
    sim: Simulation,
    stats: Rc<RefCell<Stats>>,
}

impl ServerlessSimulation {
    pub fn new(
        mut sim: Simulation,
        idle_deployer: Option<Box<dyn IdleDeployer>>,
        coldstart_policy: Option<Rc<RefCell<dyn ColdStartPolicy>>>,
        scheduler: Option<Box<dyn Scheduler>>,
    ) -> Self {
        let coldstart = coldstart_policy.unwrap_or(Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(0.0, 0.0))));
        let stats = Rc::new(RefCell::new(Default::default()));
        let ctx = sim.create_context("entry point");
        let deployer = idle_deployer.unwrap_or(Box::new(BasicDeployer {}));
        let real_scheduler = scheduler.unwrap_or(Box::new(BasicScheduler {}));
        let function_registry: Rc<RefCell<FunctionRegistry>> = Rc::new(RefCell::new(Default::default()));
        let invocation_registry: Rc<RefCell<InvocationRegistry>> = Rc::new(RefCell::new(Default::default()));
        let controller = Rc::new(RefCell::new(Controller::new(
            coldstart,
            function_registry.clone(),
            deployer,
            invocation_registry.clone(),
            real_scheduler,
            stats.clone(),
        )));
        let controller_handler_id = sim.add_handler("controller", controller.clone());
        Self {
            controller,
            controller_handler_id,
            function_registry: function_registry.clone(),
            ctx,
            resource_name_resolver: Default::default(),
            sim,
            stats,
        }
    }

    pub fn try_resolve_resource_name(&self, name: &str) -> Option<usize> {
        self.resource_name_resolver.try_resolve(name)
    }

    pub fn create_resource(&mut self, name: &str, available: u64) -> Resource {
        Resource::new(self.resource_name_resolver.resolve(name), available)
    }

    pub fn create_resource_requirement(&mut self, name: &str, needed: u64) -> ResourceRequirement {
        ResourceRequirement::new(self.resource_name_resolver.resolve(name), needed)
    }

    pub fn get_stats(&self) -> Stats {
        self.stats.borrow().clone()
    }

    pub fn new_invoker(&mut self, logic: Option<Box<dyn InvokerLogic>>, resources: ResourceProvider) -> u64 {
        let real_logic = logic.unwrap_or(Box::new(BasicInvoker {}));
        self.controller
            .borrow_mut()
            .new_invoker(self.controller_handler_id, real_logic, resources, &mut self.sim)
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        self.function_registry.borrow_mut().new_function(f)
    }

    pub fn new_function_with_group(&mut self, g: Group) -> u64 {
        self.function_registry.borrow_mut().new_function_with_group(g)
    }

    pub fn new_group(&mut self, g: Group) -> u64 {
        self.function_registry.borrow_mut().new_group(g)
    }

    pub fn send_invocation_request(&mut self, request: InvocationRequest) {
        let time = request.time;
        self.ctx
            .emit(InvocationStartEvent { request }, self.controller_handler_id, time);
    }

    // Simulation end event is useful in case
    // you have a no-unloading policy and you
    // want metrics like wasted resource time
    // to be correct at the end of simulation
    // (of course, you have to provide correct
    // time)
    pub fn set_simulation_end(&mut self, time: f64) {
        self.ctx.emit(SimulationEndEvent {}, self.controller_handler_id, time);
    }

    pub fn step(&mut self) -> bool {
        self.sim.step()
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
        self.sim.steps(step_count)
    }

    pub fn step_for_duration(&mut self, duration: f64) {
        self.sim.step_for_duration(duration);
    }

    pub fn step_until_no_events(&mut self) {
        self.sim.step_until_no_events();
    }
}
