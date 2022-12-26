use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::context::SimulationContext;
use dslab_core::simulation::Simulation;

use crate::coldstart::ColdStartPolicy;
use crate::config::Config;
use crate::controller::Controller;
use crate::event::{InvocationStartEvent, SimulationEndEvent};
use crate::function::{Application, Function, FunctionRegistry};
use crate::host::Host;
use crate::invocation::{Invocation, InvocationRegistry, InvocationRequest};
use crate::invoker::{BasicInvoker, Invoker};
use crate::resource::{Resource, ResourceConsumer, ResourceNameResolver, ResourceProvider, ResourceRequirement};
use crate::stats::Stats;
use crate::trace::Trace;
use crate::util::Counter;

pub type HandlerId = dslab_core::component::Id;

pub struct ServerlessSimulation {
    coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    controller: Rc<RefCell<Controller>>,
    controller_id: HandlerId,
    disable_contention: bool,
    function_registry: Rc<RefCell<FunctionRegistry>>,
    host_ctr: Counter,
    invocation_registry: Rc<RefCell<InvocationRegistry>>,
    ctx: SimulationContext,
    resource_name_resolver: ResourceNameResolver,
    sim: Simulation,
    stats: Rc<RefCell<Stats>>,
}

impl ServerlessSimulation {
    pub fn new(mut sim: Simulation, config: Config) -> Self {
        let stats = Rc::new(RefCell::new(Default::default()));
        let ctx = sim.create_context("entry point");
        let function_registry: Rc<RefCell<FunctionRegistry>> = Rc::new(RefCell::new(Default::default()));
        let invocation_registry: Rc<RefCell<InvocationRegistry>> = Rc::new(RefCell::new(Default::default()));
        let controller = Rc::new(RefCell::new(Controller::new(
            function_registry.clone(),
            config.idle_deployer,
            config.scheduler,
        )));
        let controller_id = sim.add_handler("controller", controller.clone());
        let mut this_sim = Self {
            coldstart: config.coldstart_policy.box_to_rc(),
            controller,
            controller_id,
            disable_contention: config.disable_contention,
            function_registry,
            host_ctr: Default::default(),
            invocation_registry,
            ctx,
            resource_name_resolver: Default::default(),
            sim,
            stats,
        };
        for host in config.hosts {
            let resources: Vec<_> = host
                .resources
                .iter()
                .map(|x| this_sim.create_resource(&x.0, x.1))
                .collect();
            this_sim.add_host(Some(host.invoker), ResourceProvider::new(resources), host.cores);
        }
        this_sim
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

    pub fn get_invocation(&self, id: u64) -> Option<Invocation> {
        self.invocation_registry.borrow().get_invocation(id).cloned()
    }

    pub fn get_stats(&self) -> Stats {
        self.stats.borrow().clone()
    }

    pub fn add_host(&mut self, invoker: Option<Box<dyn Invoker>>, resources: ResourceProvider, cores: u32) {
        let id = self.host_ctr.increment();
        let real_invoker = invoker.unwrap_or_else(|| Box::new(BasicInvoker::new()));
        let ctx = self.sim.create_context(format!("host_{}", id));
        let host = Rc::new(RefCell::new(Host::new(
            id,
            cores,
            self.disable_contention,
            resources,
            real_invoker,
            self.function_registry.clone(),
            self.invocation_registry.clone(),
            self.coldstart.clone(),
            self.controller_id,
            self.stats.clone(),
            ctx,
        )));
        self.sim.add_handler(format!("host_{}", id), host.clone());
        self.controller.borrow_mut().add_host(host);
    }

    pub fn add_function(&mut self, f: Function) -> u64 {
        self.function_registry.borrow_mut().add_function(f)
    }

    pub fn add_app_with_single_function(&mut self, app: Application) -> u64 {
        self.function_registry.borrow_mut().add_app_with_single_function(app)
    }

    pub fn add_app(&mut self, app: Application) -> u64 {
        self.function_registry.borrow_mut().add_app(app)
    }

    pub fn load_trace(&mut self, trace: &dyn Trace) {
        for app in trace.app_iter() {
            let res = ResourceConsumer::new(
                app.container_resources
                    .iter()
                    .map(|x| self.create_resource_requirement(&x.0, x.1))
                    .collect(),
            );
            self.add_app(Application::new(
                app.concurrent_invocations,
                app.container_deployment_time,
                app.container_cpu_share,
                res,
            ));
        }
        for func in trace.function_iter() {
            self.add_function(Function::new(func));
        }
        for request in trace.request_iter() {
            self.send_invocation_request(request.id, request.duration, request.time);
        }
        if let Some(t) = trace.simulation_end() {
            self.set_simulation_end(t);
        }
    }

    pub fn send_invocation_request(&mut self, id: u64, duration: f64, time: f64) -> u64 {
        let invocation_id = self.invocation_registry.borrow_mut().register_invocation();
        self.ctx.emit(
            InvocationStartEvent {
                request: InvocationRequest {
                    func_id: id,
                    duration,
                    time,
                    id: invocation_id,
                },
            },
            self.controller_id,
            time - self.sim.time(),
        );
        invocation_id
    }

    /// Simulation end event is useful in case you have a no-unloading policy and you
    /// want metrics like wasted resource time to be correct at the end of simulation
    /// (of course, you have to provide correct time).
    pub fn set_simulation_end(&mut self, time: f64) {
        self.ctx
            .emit(SimulationEndEvent {}, self.controller_id, time - self.sim.time());
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

    pub fn event_count(&self) -> u64 {
        self.sim.event_count()
    }
}
