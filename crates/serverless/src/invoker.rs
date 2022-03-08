use core::cast;
use core::event::Event;
use core::handler::EventHandler;

use crate::deployer::DeploymentStatus;
use crate::simulation::{CorePtr, ServerlessHandler};
use crate::util::Counter;

use std::collections::HashMap;
use std::rc::Weak;

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
        let invocation = Invocation {
            request,
            container_id,
        };
        self.invocations.insert(id, invocation);
        id
    }

    pub fn get_invocation(&self, id: u64) -> Option<&Invocation> {
        self.invocations.get(&id)
    }
}

/*
 * Invoker invokes an existing function instance
 * or calls Deployer in case there is none
 */
pub trait Invoker: ServerlessHandler {
    fn invoke(&mut self, request: InvocationRequest) -> InvocationStatus;
}

// BasicInvoker tries to invoke the function
// inside the first container that fits
pub struct BasicInvoker {
    sim: CorePtr,
}

impl BasicInvoker {
    pub fn new(sim: CorePtr) -> Self {
        Self { sim }
    }
}

impl EventHandler for BasicInvoker {
    fn on(&mut self, event: Event) {
        if event.data.is::<InvocationRequest>() {
            let request = *event.data.downcast::<InvocationRequest>().unwrap();
            self.invoke(request);
        }
    }
}

impl Invoker for BasicInvoker {
    fn invoke(&mut self, request: InvocationRequest) -> InvocationStatus {
        let rc = Weak::upgrade(&self.sim).unwrap();
        let mut sim = rc.borrow_mut();
        let mut it = sim.container_mgr.get_possible_containers(request.id);
        if let Some(c) = it.next() {
            let id = c.id;
            sim.new_invocation_start_event(request, id, 0.);
            InvocationStatus::Instant
        } else {
            let d = sim.deployer.borrow_mut().deploy(request.id);
            if d.status == DeploymentStatus::Rejected {
                return InvocationStatus::Rejected;
            }
            sim.new_invocation_start_event(request, d.container_id, d.deployment_time);
            InvocationStatus::Delayed
        }
    }
}

impl ServerlessHandler for BasicInvoker {
    fn register(&mut self, sim: CorePtr) {
        self.sim = sim;
    }
}
