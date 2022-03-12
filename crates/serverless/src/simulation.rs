use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use crate::container::{Container, ContainerManager, ContainerStatus};
use crate::deployer::{BasicDeployer, Deployer};
use crate::function::{Function, FunctionManager};
use crate::host::HostManager;
use crate::invoker::{BasicInvoker, InvocationManager, InvocationRequest, Invoker};
use crate::keepalive::{FixedKeepalivePolicy, KeepalivePolicy};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::stats::Stats;

use std::cell::RefCell;
use std::rc::Rc;

struct ContainerStartHandler {
    backend: Rc<RefCell<Backend>>,
}

impl ContainerStartHandler {
    pub fn new(backend: Rc<RefCell<Backend>>) -> Self {
        Self { backend }
    }
}

impl EventHandler for ContainerStartHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<u64>() {
            let id = *event.data.downcast::<u64>().unwrap();
            let mut backend = self.backend.borrow_mut();
            let container = backend.container_mgr.get_container_mut(id);
            if let Some(container) = container {
                if container.status == ContainerStatus::Deploying {
                    container.status = ContainerStatus::Idle;
                }
            }
        }
    }
}

struct ContainerEndHandler {
    backend: Rc<RefCell<Backend>>,
}

impl ContainerEndHandler {
    pub fn new(backend: Rc<RefCell<Backend>>) -> Self {
        Self { backend }
    }
}

impl EventHandler for ContainerEndHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<(u64, u64)>() {
            let (cont_id, ctr) = *event.data.downcast::<(u64, u64)>().unwrap();
            let mut backend = self.backend.borrow_mut();
            let cont = backend.container_mgr.get_container(cont_id).unwrap();
            if cont.status == ContainerStatus::Idle && cont.finished_invocations.curr() == ctr + 1 {
                let host_id = cont.host_id;
                backend.delete_container(cont_id);
            }
        }
    }
}

struct InvocationStartHandler {
    backend: Rc<RefCell<Backend>>,
    ctx: Rc<RefCell<ServerlessContext>>,
}

impl InvocationStartHandler {
    pub fn new(backend: Rc<RefCell<Backend>>, ctx: Rc<RefCell<ServerlessContext>>) -> Self {
        Self { backend, ctx }
    }
}

impl EventHandler for InvocationStartHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<(InvocationRequest, u64)>() {
            let (request, cont_id) = *event.data.downcast::<(InvocationRequest, u64)>().unwrap();
            let mut backend = self.backend.borrow_mut();
            let inv_id = backend.invocation_mgr.new_invocation(request, cont_id);
            let container = backend.container_mgr.get_container_mut(cont_id);
            if let Some(container) = container {
                container.status = ContainerStatus::Running;
                container.invocation = Some(inv_id);
                self.ctx.borrow_mut().new_invocation_end_event(inv_id, request.duration);
            }
        }
    }
}

struct InvocationEndHandler {
    backend: Rc<RefCell<Backend>>,
    ctx: Rc<RefCell<ServerlessContext>>,
}

impl InvocationEndHandler {
    pub fn new(backend: Rc<RefCell<Backend>>, ctx: Rc<RefCell<ServerlessContext>>) -> Self {
        Self { backend, ctx }
    }
}

impl EventHandler for InvocationEndHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<u64>() {
            let id = *event.data.downcast::<u64>().unwrap();
            let mut backend = self.backend.borrow_mut();
            if let Some(invocation) = backend.invocation_mgr.get_invocation(id) {
                let cont_id = invocation.container_id;
                let container = backend.container_mgr.get_container_mut(cont_id).unwrap();
                if let Some(id0) = container.invocation {
                    if id0 == id {
                        let fin = container.end_invocation();
                        let immut_container = backend.container_mgr.get_container(cont_id).unwrap();
                        let keepalive = backend.keepalive.borrow_mut().keepalive_period(immut_container);
                        self.ctx.borrow_mut().new_container_end_event(cont_id, fin, keepalive);
                    }
                }
            }
        }
    }
}

pub struct Backend {
    pub container_mgr: ContainerManager,
    pub function_mgr: FunctionManager,
    pub host_mgr: HostManager,
    pub invocation_mgr: InvocationManager,
    pub keepalive: Rc<RefCell<dyn KeepalivePolicy>>,
    pub stats: Stats,
}

impl Backend {
    pub fn new_container(
        &mut self,
        func_id: u64,
        deployment_time: f64,
        host_id: u64,
        status: ContainerStatus,
        resources: ResourceConsumer,
    ) -> &Container {
        self.container_mgr
            .new_container(&mut self.host_mgr, func_id, deployment_time, host_id, status, resources)
    }

    pub fn delete_container(&mut self, cont_id: u64) {
        let cont = self.container_mgr.get_container(cont_id).unwrap();
        self.host_mgr.get_host_mut(cont.host_id).unwrap().delete_container(cont);
        self.container_mgr.destroy_container(cont_id);
    }
}

pub struct ServerlessContext {
    pub sim_ctx: SimulationContext,
}

impl ServerlessContext {
    pub fn new(sim_ctx: SimulationContext) -> Self {
        Self { sim_ctx }
    }

    pub fn new_deploy_event(&mut self, id: u64, delay: f64) {
        self.sim_ctx.emit(id, "container_started", delay);
    }

    pub fn new_container_end_event(&mut self, id: u64, ctr: u64, delay: f64) {
        self.sim_ctx.emit((id, ctr), "container_ended", delay);
    }

    pub fn new_invocation_start_event(&mut self, request: InvocationRequest, cont_id: u64, delay: f64) {
        self.sim_ctx.emit((request, cont_id), "invocation_started", delay);
    }

    pub fn new_invocation_end_event(&mut self, inv_id: u64, delay: f64) {
        self.sim_ctx.emit(inv_id, "invocation_ended", delay);
    }
}

pub struct ServerlessSimulation {
    backend: Rc<RefCell<Backend>>,
    deployer: Rc<RefCell<dyn Deployer>>,
    extra_handlers: Vec<Rc<RefCell<dyn EventHandler>>>,
    invoker: Rc<RefCell<dyn Invoker>>,
    sim: Simulation,
    ctx: Rc<RefCell<ServerlessContext>>,
}

impl ServerlessSimulation {
    pub fn new(mut sim: Simulation) -> Self {
        let backend = Rc::new(RefCell::new(Backend {
            container_mgr: Default::default(),
            function_mgr: Default::default(),
            host_mgr: Default::default(),
            invocation_mgr: Default::default(),
            keepalive: Rc::new(RefCell::new(FixedKeepalivePolicy::new(2.0))),
            stats: Default::default(),
        }));
        let ctx = Rc::new(RefCell::new(ServerlessContext::new(
            sim.create_context("serverless simulation"),
        )));
        let deployer = Rc::new(RefCell::new(BasicDeployer::new(backend.clone(), ctx.clone())));
        let invoker = Rc::new(RefCell::new(BasicInvoker::new(
            backend.clone(),
            ctx.clone(),
            deployer.clone(),
        )));
        sim.add_handler("invocation_request", invoker.clone());
        let mut extra_handlers = Vec::<Rc<RefCell<dyn EventHandler>>>::new();
        let container_start_handler = Rc::new(RefCell::new(ContainerStartHandler::new(backend.clone())));
        sim.add_handler("container_started", container_start_handler.clone());
        extra_handlers.push(container_start_handler);
        let container_end_handler = Rc::new(RefCell::new(ContainerEndHandler::new(backend.clone())));
        sim.add_handler("container_ended", container_end_handler.clone());
        extra_handlers.push(container_end_handler);
        let invocation_start_handler = Rc::new(RefCell::new(InvocationStartHandler::new(backend.clone(), ctx.clone())));
        sim.add_handler("invocation_started", invocation_start_handler.clone());
        extra_handlers.push(invocation_start_handler);
        let invocation_end_handler = Rc::new(RefCell::new(InvocationEndHandler::new(backend.clone(), ctx.clone())));
        sim.add_handler("invocation_ended", invocation_end_handler.clone());
        extra_handlers.push(invocation_end_handler);
        Self {
            backend,
            deployer,
            extra_handlers,
            invoker,
            sim,
            ctx,
        }
    }

    pub fn get_stats(&self) -> Stats {
        self.backend.borrow().stats
    }

    pub fn new_host(&mut self, resources: ResourceProvider) -> u64 {
        self.backend.borrow_mut().host_mgr.new_host(resources)
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        self.backend.borrow_mut().function_mgr.new_function(f)
    }

    pub fn send_invocation_request(&mut self, time: f64, request: InvocationRequest) {
        self.ctx.borrow_mut().sim_ctx.emit(request, "invocation_request", time);
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
