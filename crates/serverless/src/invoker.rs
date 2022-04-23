use crate::container::ContainerStatus;
use crate::host::Host;
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
    fn dequeue(&mut self, host: &mut Host, time: f64);

    /// invoke or queue new invocation request
    fn invoke(&mut self, host: &mut Host, request: InvocationRequest, time: f64) -> InvocationStatus;
}

pub struct BasicInvoker {
    queue: Vec<InvocationRequest>,
}

impl BasicInvoker {
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    fn try_invoke_inner(&mut self, host: &mut Host, request: InvocationRequest, time: f64) -> InvocationStatus {
        let fr = host.function_registry.clone();
        let function_registry = fr.borrow();
        let app_id = function_registry.get_function(request.id).unwrap().app_id;
        let app = function_registry.get_app(app_id).unwrap();
        let mut nearest: Option<u64> = None;
        let mut wait = 0.0;
        for c in host.get_possible_containers(app) {
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
            if host.get_container(id).unwrap().status == ContainerStatus::Idle {
                return InvocationStatus::Warm(id);
            } else {
                return InvocationStatus::Cold((id, wait));
            }
        }
        if let Some((id, delay)) = host.try_deploy(app, time) {
            return InvocationStatus::Cold((id, delay));
        }
        return InvocationStatus::Rejected;
    }

    fn try_invoke(&mut self, host: &mut Host, request: InvocationRequest, time: f64) -> InvocationStatus {
        let status = self.try_invoke_inner(host, request, time);
        host.process_response(request, status, time);
        status
    }
}

impl Invoker for BasicInvoker {
    fn dequeue(&mut self, host: &mut Host, time: f64) {
        if self.queue.is_empty() {
            return;
        }
        let mut new_queue = Vec::new();
        for item in self.queue.clone().drain(..) {
            let status = self.try_invoke(host, item, time);
            if status == InvocationStatus::Rejected {
                new_queue.push(item);
            }
        }
        self.queue = new_queue;
    }

    fn invoke(&mut self, host: &mut Host, request: InvocationRequest, time: f64) -> InvocationStatus {
        let status = self.try_invoke(host, request, time);
        if status == InvocationStatus::Rejected {
            self.queue.push(request);
            return InvocationStatus::Queued;
        }
        status
    }
}
