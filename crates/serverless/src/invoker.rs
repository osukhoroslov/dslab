use core::event::Event;
use core::handler::EventHandler;

use crate::container::ContainerStatus;
use crate::deployer::{Deployer, DeploymentStatus};
use crate::simulation::{Backend, ServerlessContext};
use crate::stats::Stats;
use crate::util::Counter;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(PartialEq)]
pub enum InvocationStatus {
    Instant(u64),
    Delayed(f64),
    Rejected,
}

#[derive(Copy, Clone, Debug)]
pub struct InvocationRequest {
    pub id: u64,
    pub duration: f64,
    pub time: f64,
}

#[derive(Copy, Clone)]
pub struct Invocation {
    pub request: InvocationRequest,
    pub container_id: u64,
    pub finished: Option<f64>,
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
            finished: None,
        };
        self.invocations.insert(id, invocation);
        id
    }

    pub fn get_invocation(&self, id: u64) -> Option<&Invocation> {
        self.invocations.get(&id)
    }

    pub fn get_invocation_mut(&mut self, id: u64) -> Option<&mut Invocation> {
        self.invocations.get_mut(&id)
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

    pub fn start_invocation(&mut self, cont_id: u64, request: InvocationRequest, time: f64) {
        let mut backend = self.backend.borrow_mut();
        let inv_id = backend.invocation_mgr.new_invocation(request, cont_id);
        let container = backend.container_mgr.get_container_mut(cont_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let delta = time - container.last_change;
            self.stats
                .borrow_mut()
                .update_wasted_resources(delta, &container.resources);
        }
        container.last_change = time;
        container.status = ContainerStatus::Running;
        container.invocations.insert(inv_id);
        self.ctx.borrow_mut().new_invocation_end_event(inv_id, request.duration);
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
                } else if let InvocationStatus::Instant(cont_id) = status {
                    drop(stats);
                    self.start_invocation(cont_id, request, event.time.into_inner());
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
        _ctx: Rc<RefCell<ServerlessContext>>,
        deployer: Rc<RefCell<Deployer>>,
        curr_time: f64,
    ) -> InvocationStatus {
        let mut backend_ = backend.borrow_mut();
        let group_id = backend_.function_mgr.get_function(request.id).unwrap().group_id;
        let group = backend_.function_mgr.get_group(group_id).unwrap();
        let it = backend_.container_mgr.get_possible_containers(group);
        let mut nearest: Option<u64> = None;
        let mut wait = 0.0;
        for c in it {
            let delay = if c.status == ContainerStatus::Deploying {
                c.deployment_time + c.last_change - curr_time
            } else {
                0.0
            };
            if nearest.is_none() || wait > delay {
                wait = delay;
                nearest = Some(c.id);
            }
        }
        if let Some(id) = nearest {
            if backend_.container_mgr.get_container(id).unwrap().status == ContainerStatus::Idle {
                InvocationStatus::Instant(id)
            } else {
                backend_.container_mgr.reserve_container(id, request);
                InvocationStatus::Delayed(wait)
            }
        } else {
            drop(backend_);
            let d = deployer.borrow_mut().deploy(group_id, Some(request), curr_time);
            if d.status == DeploymentStatus::Rejected {
                return InvocationStatus::Rejected;
            }
            InvocationStatus::Delayed(d.deployment_time)
        }
    }
}
