use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use core::simulation::Simulation;

use crate::container::{Container, ContainerManager, ContainerStatus};
use crate::deployer::{BasicDeployer, Deployer};
use crate::function::{Function, FunctionManager};
use crate::host::HostManager;
use crate::invoker::{BasicInvoker, Invocation, InvocationManager, InvocationRequest, Invoker};

use std::boxed::Box;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

pub type CorePtr = Weak<RefCell<ServerlessSimulationCore>>;

pub trait ServerlessHandler: EventHandler {
    fn register(&mut self, sim: CorePtr);
}

struct ContainerStartHandler {
    sim: CorePtr,
}

impl EventHandler for ContainerStartHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<u64>() {
            let id = *event.data.downcast::<u64>().unwrap();
            let rc = Weak::upgrade(&self.sim).unwrap();
            let mut sim = rc.borrow_mut();
            let mut container = sim.container_mgr.get_container_mut(id);
            if let Some(container) = container {
                if container.status == ContainerStatus::Deploying {
                    container.status = ContainerStatus::Idle;
                }
            }
        }
    }
}

impl ServerlessHandler for ContainerStartHandler {
    fn register(&mut self, sim: CorePtr) {
        self.sim = sim;
    }
}

struct InvocationStartHandler {
    sim: CorePtr,
}

impl EventHandler for InvocationStartHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<(InvocationRequest, u64)>() {
            let (request, cont_id) = *event.data.downcast::<(InvocationRequest, u64)>().unwrap();
            let rc = Weak::upgrade(&self.sim).unwrap();
            let mut sim = rc.borrow_mut();
            let inv_id = sim.invocation_mgr.new_invocation(request, cont_id);
            let mut container = sim.container_mgr.get_container_mut(cont_id);
            if let Some(container) = container {
                container.status = ContainerStatus::Running;
                container.invocation = Some(inv_id);
                sim.new_invocation_end_event(inv_id, request.duration);
            }
        }
    }
}

impl ServerlessHandler for InvocationStartHandler {
    fn register(&mut self, sim: CorePtr) {
        self.sim = sim;
    }
}

struct InvocationEndHandler {
    sim: CorePtr,
}

impl EventHandler for InvocationEndHandler {
    fn on(&mut self, event: Event) {
        if event.data.is::<u64>() {
            let id = *event.data.downcast::<u64>().unwrap();
            let rc = Weak::upgrade(&self.sim).unwrap();
            let mut sim = rc.borrow_mut();
            if let Some(invocation) = sim.invocation_mgr.get_invocation(id) {
                let cont_id = invocation.container_id;
                let mut container = sim.container_mgr.get_container_mut(cont_id).unwrap();
                if let Some(id0) = container.invocation {
                    if id0 == id {
                        container.invocation = None;
                        container.status = ContainerStatus::Idle;
                    }
                }
            }
        }
    }
}

impl ServerlessHandler for InvocationEndHandler {
    fn register(&mut self, sim: CorePtr) {
        self.sim = sim;
    }
}

pub struct ServerlessSimulationCore {
    pub container_mgr: ContainerManager,
    pub deployer: Rc<RefCell<dyn Deployer>>,
    pub function_mgr: FunctionManager,
    pub host_mgr: HostManager,
    pub invocation_mgr: InvocationManager,
    pub invoker: Rc<RefCell<dyn Invoker>>,
    pub extra_handlers: Vec<Rc<RefCell<dyn ServerlessHandler>>>,
    pub sim: Simulation,
    pub sim_ctx: SimulationContext,
}

impl ServerlessSimulationCore {
    pub fn new(mut sim: Simulation) -> Self {
        let invoker = Rc::new(RefCell::new(BasicInvoker::new(Weak::new())));
        sim.add_handler("invocation_request", invoker.clone());
        let deployer = Rc::new(RefCell::new(BasicDeployer::new(Weak::new())));
        let mut extra_handlers = Vec::<Rc<RefCell<dyn ServerlessHandler>>>::new();
        let container_start_handler = Rc::new(RefCell::new(ContainerStartHandler { sim: Weak::new() }));
        sim.add_handler("container_started", container_start_handler.clone());
        extra_handlers.push(container_start_handler);
        let invocation_start_handler = Rc::new(RefCell::new(InvocationStartHandler { sim: Weak::new() }));
        sim.add_handler("invocation_started", invocation_start_handler.clone());
        extra_handlers.push(invocation_start_handler);
        let invocation_end_handler = Rc::new(RefCell::new(InvocationEndHandler { sim: Weak::new() }));
        sim.add_handler("invocation_ended", invocation_end_handler.clone());
        extra_handlers.push(invocation_end_handler);
        let sim_ctx = sim.create_context("serverless simulation");
        Self {
            container_mgr: Default::default(),
            deployer,
            function_mgr: Default::default(),
            host_mgr: Default::default(),
            invocation_mgr: Default::default(),
            invoker,
            extra_handlers,
            sim,
            sim_ctx,
        }
    }

    pub fn set_ptr_to_self(&mut self, self_ptr: CorePtr) {
        for handler in self.extra_handlers.iter() {
            handler.borrow_mut().register(self_ptr.clone());
        }
        self.deployer.borrow_mut().register(self_ptr.clone());
        self.invoker.borrow_mut().register(self_ptr);
    }

    pub fn new_deploy_event(&mut self, id: u64, delay: f64) {
        self.sim_ctx.emit(id, "container_started", delay);
    }

    pub fn new_invocation_start_event(&mut self, request: InvocationRequest, cont_id: u64, delay: f64) {
        self.sim_ctx.emit((request, cont_id), "invocation_started", delay);
    }

    pub fn new_invocation_end_event(&mut self, inv_id: u64, delay: f64) {
        self.sim_ctx.emit(inv_id, "invocation_ended", delay);
    }
}

pub struct ServerlessSimulation {
    core: Rc<RefCell<ServerlessSimulationCore>>,
}

impl ServerlessSimulation {
    pub fn new(mut sim: Simulation) -> Self {
        let core = Rc::new(RefCell::new(ServerlessSimulationCore::new(sim)));
        core.borrow_mut().set_ptr_to_self(Rc::downgrade(&core));
        Self { core }
    }

    pub fn new_host(&mut self) -> u64 {
        self.core.borrow_mut().host_mgr.new_host()
    }

    pub fn new_function(&mut self, f: Function) -> u64 {
        self.core.borrow_mut().function_mgr.new_function(f)
    }

    pub fn send_invocation_request(&mut self, time: f64, request: InvocationRequest) {
        self.core.borrow_mut().sim_ctx.emit(request, "invocation_request", time);
    }

}
