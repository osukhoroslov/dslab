use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use simcore::cast;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::simulation::Simulation;

use crate::coldstart::ColdStartPolicy;
use crate::container::ContainerStatus;
use crate::event::{ContainerEndEvent, ContainerStartEvent, InvocationEndEvent};
use crate::function::{FunctionRegistry, Group};
use crate::host::Host;
use crate::invocation::{InvocationRegistry, InvocationRequest};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::simulation::HandlerId;
use crate::stats::Stats;

type InvocationQueue = Vec<InvocationRequest>;

#[derive(Clone, Copy, PartialEq)]
pub enum InvocationStatus {
    Warm(u64),
    Cold((u64, f64)),
    Queued,
    Rejected,
}

pub trait InvokerLogic {
    // try to invoke some of the queued functions
    fn dequeue(&mut self, host: &mut Host, queue: &mut InvocationQueue, time: f64);

    // invoke or queue new invocation request
    fn invoke(
        &mut self,
        host: &mut Host,
        queue: &mut InvocationQueue,
        request: InvocationRequest,
        time: f64,
    ) -> InvocationStatus;
}

/*
 * Invoker handles invocations at host level.
 * It chooses containers for execution and deploys new containers.
 * It also manages host invocation queue.
 */
pub struct Invoker {
    host: Host,
    logic: Box<dyn InvokerLogic>,
    queue: InvocationQueue,
}

impl Invoker {
    pub fn new(
        id: u64,
        coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
        controller_id: HandlerId,
        function_registry: Rc<RefCell<FunctionRegistry>>,
        invocation_registry: Rc<RefCell<InvocationRegistry>>,
        logic: Box<dyn InvokerLogic>,
        resources: ResourceProvider,
        sim: &mut Simulation,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        let ctx = sim.create_context(format!("host_{}", id));
        let host = Host::new(
            id,
            coldstart,
            controller_id,
            ctx,
            function_registry,
            invocation_registry,
            resources,
            stats,
        );
        Self {
            host,
            logic,
            queue: Default::default(),
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.host.can_allocate(resources)
    }

    pub fn can_invoke(&self, group: &Group) -> bool {
        self.host.can_invoke(group)
    }

    pub fn invoke(&mut self, request: InvocationRequest, time: f64) -> InvocationStatus {
        self.logic.invoke(&mut self.host, &mut self.queue, request, time)
    }

    pub fn setup_handler(&mut self, handler_id: HandlerId) {
        self.host.invoker_handler_id = handler_id;
    }

    pub fn try_deploy(&mut self, group: &Group, time: f64) -> Option<(u64, f64)> {
        self.host.try_deploy(group, time)
    }

    pub fn update_end_metrics(&mut self, time: f64) {
        self.host.update_end_metrics(time);
    }
}

impl EventHandler for Invoker {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            ContainerStartEvent { id } => {
                self.host.start_container(id, event.time);
                self.logic.dequeue(&mut self.host, &mut self.queue, event.time);
            }
            ContainerEndEvent { id, expected_count } => {
                self.host.end_container(id, expected_count, event.time);
            }
            InvocationEndEvent { id } => {
                self.host.end_invocation(id, event.time);
                self.logic.dequeue(&mut self.host, &mut self.queue, event.time);
            }
        });
    }
}

pub struct BasicInvoker {}

impl BasicInvoker {
    fn try_invoke_inner(&mut self, host: &mut Host, request: InvocationRequest, time: f64) -> InvocationStatus {
        let fr = host.function_registry.clone();
        let function_registry = fr.borrow();
        let group_id = function_registry.get_function(request.id).unwrap().group_id;
        let group = function_registry.get_group(group_id).unwrap();
        let mut nearest: Option<u64> = None;
        let mut wait = 0.0;
        for c in host.get_possible_containers(group) {
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
        if let Some((id, delay)) = host.try_deploy(group, time) {
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

impl InvokerLogic for BasicInvoker {
    fn dequeue(&mut self, host: &mut Host, queue: &mut InvocationQueue, time: f64) {
        if queue.is_empty() {
            return;
        }
        let mut new_queue = Vec::new();
        for item in queue.drain(..) {
            let status = self.try_invoke(host, item, time);
            if status == InvocationStatus::Rejected {
                new_queue.push(item);
            }
        }
        *queue = new_queue;
    }

    fn invoke(
        &mut self,
        host: &mut Host,
        queue: &mut InvocationQueue,
        request: InvocationRequest,
        time: f64,
    ) -> InvocationStatus {
        let status = self.try_invoke(host, request, time);
        if status == InvocationStatus::Rejected {
            queue.push(request);
            return InvocationStatus::Queued;
        }
        status
    }
}
