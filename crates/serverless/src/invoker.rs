use core::event::Event;
use core::handler::EventHandler;

use crate::deployer::{Deployer, DeploymentStatus};
use crate::simulation::{Backend, ServerlessContext};
use crate::stats::Stats;
use crate::util::Counter;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(PartialEq)]
pub enum InvocationStatus {
    Instant,
    Delayed(f64),
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
        curr_time: f64,
    ) -> InvocationStatus;
}

pub struct Invoker {
    backend: Rc<RefCell<Backend>>,
    core: Box<dyn InvokerCore>,
    ctx: Rc<RefCell<ServerlessContext>>,
    deployer: Rc<RefCell<Deployer>>,
    stats: Rc<RefCell<Stats>>,
}

impl Invoker {
    pub fn new(
        backend: Rc<RefCell<Backend>>,
        core: Box<dyn InvokerCore>,
        ctx: Rc<RefCell<ServerlessContext>>,
        deployer: Rc<RefCell<Deployer>>,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        Self {
            backend,
            core,
            ctx,
            deployer,
            stats,
        }
    }
}

impl EventHandler for Invoker {
    fn on(&mut self, event: Event) {
        if event.data.is::<InvocationRequest>() {
            let request = *event.data.downcast::<InvocationRequest>().unwrap();
            let status = self.core.invoke(
                request,
                self.backend.clone(),
                self.ctx.clone(),
                self.deployer.clone(),
                event.time.into_inner(),
            );
            if status != InvocationStatus::Rejected {
                let mut stats = self.stats.borrow_mut();
                stats.invocations += 1;
                if let InvocationStatus::Delayed(delay) = status {
                    stats.cold_starts_total_time += delay;
                    stats.cold_starts += 1;
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
        curr_time: f64,
    ) -> InvocationStatus {
        let backend_ = backend.borrow();
        let mut it = backend_.container_mgr.get_possible_containers(request.id);
        if let Some(c) = it.next() {
            let id = c.id;
            ctx.borrow_mut().new_invocation_start_event(request, id, 0.);
            InvocationStatus::Instant
        } else {
            drop(backend_);
            let d = deployer.borrow_mut().deploy(request.id, curr_time);
            if d.status == DeploymentStatus::Rejected {
                return InvocationStatus::Rejected;
            }
            ctx.borrow_mut()
                .new_invocation_start_event(request, d.container_id, d.deployment_time);
            InvocationStatus::Delayed(d.deployment_time)
        }
    }
}
