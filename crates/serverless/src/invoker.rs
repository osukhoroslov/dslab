use core::event::Event;
use core::handler::EventHandler;

use crate::deployer::{Deployer, DeploymentStatus};
use crate::simulation::{Backend, ServerlessContext};
use crate::util::Counter;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Eq, PartialEq)]
pub enum InvocationStatus {
    Instant,
    Delayed,
    Rejected,
}

#[derive(Copy, Clone, Debug)]
pub struct InvocationRequest {
    pub id: u64,
    pub duration: f64,
}

#[derive(Copy, Clone)]
pub struct Invocation {
    pub request: InvocationRequest,
    pub container_id: u64,
}

#[derive(Default)]
pub struct InvocationManager {
    invocation_ctr: Counter,
    invocations: HashMap<u64, Invocation>,
}

impl InvocationManager {
    pub fn new_invocation(&mut self, request: InvocationRequest, container_id: u64) -> u64 {
        let id = self.invocation_ctr.next();
        let invocation = Invocation { request, container_id };
        self.invocations.insert(id, invocation);
        id
    }

    pub fn get_invocation(&self, id: u64) -> Option<&Invocation> {
        self.invocations.get(&id)
    }
}

/*
 * InvokerCore invokes an existing function instance
 * or calls Deployer in case there is none
 */
pub trait InvokerCore {
    fn invoke(
        &mut self,
        request: InvocationRequest,
        backend: Rc<RefCell<Backend>>,
        ctx: Rc<RefCell<ServerlessContext>>,
        deployer: Rc<RefCell<Deployer>>,
    ) -> InvocationStatus;
}

pub struct Invoker {
    backend: Rc<RefCell<Backend>>,
    core: Box<dyn InvokerCore>,
    ctx: Rc<RefCell<ServerlessContext>>,
    deployer: Rc<RefCell<Deployer>>,
}

impl Invoker {
    pub fn new(
        backend: Rc<RefCell<Backend>>,
        core: Box<dyn InvokerCore>,
        ctx: Rc<RefCell<ServerlessContext>>,
        deployer: Rc<RefCell<Deployer>>,
    ) -> Self {
        Self {
            backend,
            core,
            ctx,
            deployer,
        }
    }
}

impl EventHandler for Invoker {
    fn on(&mut self, event: Event) {
        if event.data.is::<InvocationRequest>() {
            let request = *event.data.downcast::<InvocationRequest>().unwrap();
            let status = self
                .core
                .invoke(request, self.backend.clone(), self.ctx.clone(), self.deployer.clone());
            if status != InvocationStatus::Rejected {
                let mut backend = self.backend.borrow_mut();
                backend.stats.invocations += 1;
                if status == InvocationStatus::Delayed {
                    backend.stats.cold_starts += 1;
                }
            }
        }
    }
}

// BasicInvoker tries to invoke the function
// inside the first container that fits
pub struct BasicInvoker {}

impl InvokerCore for BasicInvoker {
    fn invoke(
        &mut self,
        request: InvocationRequest,
        backend: Rc<RefCell<Backend>>,
        ctx: Rc<RefCell<ServerlessContext>>,
        deployer: Rc<RefCell<Deployer>>,
    ) -> InvocationStatus {
        let mut backend_ = backend.borrow_mut();
        let mut it = backend_.container_mgr.get_possible_containers(request.id);
        if let Some(c) = it.next() {
            let id = c.id;
            ctx.borrow_mut().new_invocation_start_event(request, id, 0.);
            InvocationStatus::Instant
        } else {
            drop(backend_);
            let d = deployer.borrow_mut().deploy(request.id);
            if d.status == DeploymentStatus::Rejected {
                return InvocationStatus::Rejected;
            }
            backend_ = backend.borrow_mut();
            backend_.stats.cold_starts_total_time += d.deployment_time;
            ctx.borrow_mut()
                .new_invocation_start_event(request, d.container_id, d.deployment_time);
            InvocationStatus::Delayed
        }
    }
}
