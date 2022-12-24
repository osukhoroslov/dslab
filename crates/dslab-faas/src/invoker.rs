use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use crate::container::{ContainerManager, ContainerStatus};
use crate::function::FunctionRegistry;
use crate::invocation::InvocationRequest;

#[derive(Clone, Copy, PartialEq)]
pub enum InvocationStatus {
    Warm(u64),
    Cold((u64, f64)),
    Queued,
    Rejected,
}

/// Invoker handles invocations at host level.
/// It chooses containers for execution, deploys new containers and manages invocation queue.
pub trait Invoker {
    /// try to invoke some of the queued functions
    fn dequeue(&mut self, fr: Rc<RefCell<FunctionRegistry>>, cm: &mut ContainerManager, time: f64);

    /// invoke or queue new invocation request
    fn invoke(
        &mut self,
        request: InvocationRequest,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvocationStatus;

    fn to_string(&self) -> String {
        "STUB INVOKER NAME".to_string()
    }
}

#[derive(Default)]
pub struct BasicInvoker {
    queue: Vec<InvocationRequest>,
}

impl BasicInvoker {
    pub fn new() -> Self {
        Default::default()
    }

    fn try_invoke(
        &mut self,
        request: InvocationRequest,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvocationStatus {
        let fr = fr.borrow();
        let app = fr.get_app_by_function(request.func_id).unwrap();
        let mut nearest: Option<u64> = None;
        let mut wait = 0.0;
        for c in cm.get_possible_containers(app, true) {
            let delay = if c.status == ContainerStatus::Deploying {
                c.deployment_time + c.last_change - time
            } else {
                0.0
            };
            if nearest.is_none() || wait > delay {
                wait = delay;
                nearest = Some(c.id);
            }
        }
        if let Some(id) = nearest {
            if cm.get_container(id).unwrap().status == ContainerStatus::Idle {
                return InvocationStatus::Warm(id);
            } else {
                return InvocationStatus::Cold((id, wait));
            }
        }
        if let Some((id, delay)) = cm.try_deploy(app, time) {
            return InvocationStatus::Cold((id, delay));
        }
        InvocationStatus::Rejected
    }
}

impl Invoker for BasicInvoker {
    fn dequeue(&mut self, fr: Rc<RefCell<FunctionRegistry>>, cm: &mut ContainerManager, time: f64) {
        if self.queue.is_empty() {
            return;
        }
        let mut new_queue = Vec::new();
        for item in self.queue.clone().drain(..) {
            let status = self.try_invoke(item, fr.clone(), cm, time);
            if status == InvocationStatus::Rejected {
                new_queue.push(item);
            }
        }
        self.queue = new_queue;
    }

    fn invoke(
        &mut self,
        request: InvocationRequest,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvocationStatus {
        let status = self.try_invoke(request, fr, cm, time);
        if status == InvocationStatus::Rejected {
            self.queue.push(request);
            return InvocationStatus::Queued;
        }
        status
    }

    fn to_string(&self) -> String {
        "BasicInvoker".to_string()
    }
}

pub fn default_invoker_resolver(s: &str) -> Box<dyn Invoker> {
    if s == "BasicInvoker" {
        Box::new(BasicInvoker::new())
    } else {
        panic!("Can't resolve: {}", s);
    }
}
