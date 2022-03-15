use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use crate::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use crate::container::{Container, ContainerManager, ContainerStatus};
use crate::deployer::{BasicDeployer, Deployer, DeployerCore};
use crate::function::{Function, FunctionManager, Group};
use crate::host::HostManager;
use crate::invoker::{BasicInvoker, InvocationManager, InvocationRequest, Invoker, InvokerCore};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::stats::Stats;

use std::cell::RefCell;
use std::rc::Rc;

struct ContainerStartHandler {
    backend: Rc<RefCell<Backend>>,
    ctx: Rc<RefCell<ServerlessContext>>,
    invoker: Rc<RefCell<Invoker>>,
}

impl ContainerStartHandler {
    pub fn new(
        backend: Rc<RefCell<Backend>>,
        ctx: Rc<RefCell<ServerlessContext>>,
        invoker: Rc<RefCell<Invoker>>,
    ) -> Self {
        Self { backend, ctx, invoker }
    }
}

impl EventHandler for ContainerStartHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<(u64, Option<InvocationRequest>)>() {
            let (id, mut invocation) = *event.data.downcast::<(u64, Option<InvocationRequest>)>().unwrap();
            let mut backend = self.backend.borrow_mut();
            if let Some(stolen) = backend.container_mgr.get_stolen_prewarm(id) {
                invocation = Some(stolen);
            }
            let container = backend.container_mgr.get_container_mut(id).unwrap();
            if let Some(invocation) = invocation {
                let cont_id = container.id;
                drop(backend);
                self.invoker
                    .borrow_mut()
                    .start_invocation(cont_id, invocation, event.time.into_inner());
            } else {
                container.status = ContainerStatus::Idle;
                let immut_container = backend.container_mgr.get_container(id).unwrap();
                let keepalive = backend.coldstart.borrow_mut().keepalive_window(immut_container);
                self.ctx.borrow_mut().new_container_end_event(id, 0, keepalive);
            }
        }
    }
}

struct ContainerEndHandler {
    backend: Rc<RefCell<Backend>>,
    stats: Rc<RefCell<Stats>>,
}

impl ContainerEndHandler {
    pub fn new(backend: Rc<RefCell<Backend>>, stats: Rc<RefCell<Stats>>) -> Self {
        Self { backend, stats }
    }
}

impl EventHandler for ContainerEndHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<(u64, u64)>() {
            let (cont_id, ctr) = *event.data.downcast::<(u64, u64)>().unwrap();
            let mut backend = self.backend.borrow_mut();
            let cont = backend.container_mgr.get_container(cont_id).unwrap();
            if cont.status == ContainerStatus::Idle && cont.finished_invocations.curr() == ctr {
                let delta = event.time.into_inner() - cont.last_change;
                self.stats.borrow_mut().update_wasted_resources(delta, &cont.resources);
                backend.delete_container(cont_id);
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
                        let fin = 1 + container.end_invocation(event.time.into_inner());
                        let func_id = backend.invocation_mgr.get_invocation(id).unwrap().request.id;
                        let group_id = backend.function_mgr.get_function(func_id).unwrap().group_id;
                        let group = backend.function_mgr.get_group(group_id).unwrap();
                        let prewarm = backend.coldstart.borrow_mut().prewarm_window(group);
                        if prewarm != Some(0.) {
                            if let Some(prewarm) = prewarm {
                                self.ctx.borrow_mut().new_idle_deploy_event(group_id, prewarm);
                            }
                            let immut_container = backend.container_mgr.get_container(cont_id).unwrap();
                            let keepalive = backend.coldstart.borrow_mut().keepalive_window(immut_container);
                            self.ctx.borrow_mut().new_container_end_event(cont_id, fin, keepalive);
                        }
                    }
                }
            }
        }
    }
}

struct IdleDeployHandler {
    deployer: Rc<RefCell<Deployer>>,
}

impl IdleDeployHandler {
    pub fn new(deployer: Rc<RefCell<Deployer>>) -> Self {
        Self { deployer }
    }
}

impl EventHandler for IdleDeployHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<u64>() {
            let id = *event.data.downcast::<u64>().unwrap();
            self.deployer.borrow_mut().deploy(id, None, event.time.into_inner());
        }
    }
}

struct SimulationEndHandler {
    backend: Rc<RefCell<Backend>>,
}

impl SimulationEndHandler {
    pub fn new(backend: Rc<RefCell<Backend>>) -> Self {
        Self { backend }
    }
}

impl EventHandler for SimulationEndHandler {
    fn on(&mut self, event: Event) {
        let backend = self.backend.borrow();
        let mut stats = backend.stats.borrow_mut();
        for container in backend.container_mgr.get_containers() {
            if container.status == ContainerStatus::Idle {
                let delta = event.time.into_inner() - container.last_change;
                stats.update_wasted_resources(delta, &container.resources);
            }
        }
    }
}

pub struct Backend {
    pub container_mgr: ContainerManager,
    pub function_mgr: FunctionManager,
    pub host_mgr: HostManager,
    pub invocation_mgr: InvocationManager,
    pub coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    pub stats: Rc<RefCell<Stats>>,
}

impl Backend {
    pub fn new_container(
        &mut self,
        group_id: u64,
        deployment_time: f64,
        host_id: u64,
        status: ContainerStatus,
        resources: ResourceConsumer,
        curr_time: f64,
        prewarmed: bool,
    ) -> &Container {
        self.container_mgr.new_container(
            &mut self.host_mgr,
            group_id,
            deployment_time,
            host_id,
            status,
            resources,
            curr_time,
            prewarmed,
        )
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

    pub fn new_deploy_event(&mut self, id: u64, delay: f64, invocation: Option<InvocationRequest>) {
        self.sim_ctx.emit((id, invocation), "container_started", delay);
    }

    pub fn new_container_end_event(&mut self, id: u64, ctr: u64, delay: f64) {
        self.sim_ctx.emit((id, ctr), "container_ended", delay);
    }

    pub fn new_idle_deploy_event(&mut self, id: u64, delay: f64) {
        self.sim_ctx.emit(id, "idle_deploy", delay);
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
    ctx: Rc<RefCell<ServerlessContext>>,
    deployer: Rc<RefCell<Deployer>>,
    extra_handlers: Vec<Rc<RefCell<dyn EventHandler>>>,
    invoker: Rc<RefCell<Invoker>>,
    sim: Simulation,
    stats: Rc<RefCell<Stats>>,
}

impl ServerlessSimulation {
    pub fn new(
        mut sim: Simulation,
        deployer_core: Option<Box<dyn DeployerCore>>,
        invoker_core: Option<Box<dyn InvokerCore>>,
        coldstart_policy: Option<Rc<RefCell<dyn ColdStartPolicy>>>,
    ) -> Self {
        let coldstart = if let Some(cs) = coldstart_policy {
            cs
        } else {
            Rc::new(RefCell::new(FixedTimeColdStartPolicy::new(0.0, None)))
        };
        let stats = Rc::new(RefCell::new(Default::default()));
        let backend = Rc::new(RefCell::new(Backend {
            container_mgr: Default::default(),
            function_mgr: Default::default(),
            host_mgr: Default::default(),
            invocation_mgr: Default::default(),
            coldstart,
            stats: stats.clone(),
        }));
        let ctx = Rc::new(RefCell::new(ServerlessContext::new(
            sim.create_context("serverless simulation"),
        )));
        let deployer = Rc::new(RefCell::new(if let Some(dc) = deployer_core {
            Deployer::new(backend.clone(), dc, ctx.clone(), stats.clone())
        } else {
            Deployer::new(backend.clone(), Box::new(BasicDeployer {}), ctx.clone(), stats.clone())
        }));
        let invoker = Rc::new(RefCell::new(if let Some(inc) = invoker_core {
            Invoker::new(backend.clone(), inc, ctx.clone(), deployer.clone(), stats.clone())
        } else {
            Invoker::new(
                backend.clone(),
                Box::new(BasicInvoker {}),
                ctx.clone(),
                deployer.clone(),
                stats.clone(),
            )
        }));
        sim.add_handler("invocation_request", invoker.clone());
        let mut extra_handlers = Vec::<Rc<RefCell<dyn EventHandler>>>::new();
        let container_start_handler = Rc::new(RefCell::new(ContainerStartHandler::new(
            backend.clone(),
            ctx.clone(),
            invoker.clone(),
        )));
        sim.add_handler("container_started", container_start_handler.clone());
        extra_handlers.push(container_start_handler);
        let container_end_handler = Rc::new(RefCell::new(ContainerEndHandler::new(backend.clone(), stats.clone())));
        sim.add_handler("container_ended", container_end_handler.clone());
        extra_handlers.push(container_end_handler);
        let invocation_end_handler = Rc::new(RefCell::new(InvocationEndHandler::new(backend.clone(), ctx.clone())));
        sim.add_handler("invocation_ended", invocation_end_handler.clone());
        extra_handlers.push(invocation_end_handler);
        let idle_deploy_handler = Rc::new(RefCell::new(IdleDeployHandler::new(deployer.clone())));
        sim.add_handler("idle_deploy", idle_deploy_handler.clone());
        extra_handlers.push(idle_deploy_handler);
        let simulation_end_handler = Rc::new(RefCell::new(SimulationEndHandler::new(backend.clone())));
        sim.add_handler("simulation_end", simulation_end_handler.clone());
        extra_handlers.push(simulation_end_handler);
        Self {
            backend,
            deployer,
            extra_handlers,
            invoker,
            sim,
            ctx,
            stats,
        }
    }

    pub fn get_stats(&self) -> Stats {
        self.stats.borrow().clone()
    }

    pub fn new_host(&mut self, resources: ResourceProvider) -> u64 {
        self.backend.borrow_mut().host_mgr.new_host(resources)
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        self.backend.borrow_mut().function_mgr.new_function(f)
    }

    pub fn new_function_with_group(&mut self, g: Group) -> u64 {
        self.backend.borrow_mut().function_mgr.new_function_with_group(g)
    }

    pub fn new_group(&mut self, g: Group) -> u64 {
        self.backend.borrow_mut().function_mgr.new_group(g)
    }

    pub fn send_invocation_request(&mut self, time: f64, request: InvocationRequest) {
        self.ctx.borrow_mut().sim_ctx.emit(request, "invocation_request", time);
    }

    // Simulation end event is useful in case
    // you have a no-unloading policy and you
    // want metrics like wasted resource time
    // to be correct at the end of simulation
    // (of course, you have to provide correct
    // time)
    pub fn set_simulation_end(&mut self, time: f64) {
        self.ctx.borrow_mut().sim_ctx.emit((), "simulation_end", time);
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
