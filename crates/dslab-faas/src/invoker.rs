//! A component that manages incoming invocations on a host.
use std::boxed::Box;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use crate::container::{ContainerManager, ContainerStatus};
use crate::function::{Application, FunctionRegistry};
use crate::invocation::Invocation;
use crate::stats::Stats;

/// Invoker's decision on new function invocation.
#[derive(Clone, Copy, PartialEq)]
pub enum InvokerDecision {
    /// Invocation will instantly start running on a warm container.
    Warm(usize),
    /// Invocation will start running on a cold container after it is fully deployed.
    Cold((usize, f64)),
    /// Invocation is queued on the invoker because the invoker can't run it now or provide a cold container.
    Queued,
    /// Invoker rejects the invocation.
    /// Note: This value is used internally, invoker shouldn't return it to the host.
    Rejected,
}

/// Previously-queued invocation that is finally able to be executed.
#[derive(Clone, Copy)]
pub struct DequeuedInvocation {
    /// Invocation id.
    pub id: usize,
    /// Container id.
    pub container_id: usize,
    /// Deploying delay if the invocation will be executed on a deploying container.
    pub delay: Option<f64>,
}

impl DequeuedInvocation {
    /// Creates new DequeuedInvocation.
    pub fn new(id: usize, container_id: usize, delay: Option<f64>) -> Self {
        Self {
            id,
            container_id,
            delay,
        }
    }
}

fn try_invoke(app: &Application, cm: &mut ContainerManager, time: f64) -> InvokerDecision {
    let mut nearest: Option<usize> = None;
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
        if cm.get_container(id).unwrap().status == ContainerStatus::Deploying {
            return InvokerDecision::Cold((id, wait));
        } else {
            return InvokerDecision::Warm(id);
        }
    }
    if let Some((id, delay)) = cm.try_deploy(app, time) {
        return InvokerDecision::Cold((id, delay));
    }
    InvokerDecision::Rejected
}

/// Invoker handles invocations at host level.
/// It chooses containers for execution, deploys new containers and manages invocation queue.
pub trait Invoker {
    /// Try to invoke some of the queued functions.
    fn dequeue(
        &mut self,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        stats: &mut Stats,
        time: f64,
    ) -> Vec<DequeuedInvocation>;

    /// Invoke or queue new invocation.
    fn invoke(
        &mut self,
        invocation: &Invocation,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvokerDecision;

    /// Returns invocation queue length.
    fn queue_len(&self) -> usize;

    /// Returns a string with invoker description.
    fn to_string(&self) -> String {
        "STUB INVOKER NAME".to_string()
    }
}

#[derive(Clone, Copy)]
struct InvokerQueueItem {
    pub invocation_id: usize,
    pub func_id: usize,
    pub app_id: usize,
    pub time: f64,
}

impl InvokerQueueItem {
    pub fn new(invocation_id: usize, func_id: usize, app_id: usize, time: f64) -> Self {
        Self {
            invocation_id,
            func_id,
            app_id,
            time,
        }
    }
}

/// `NaiveInvoker` iterates over all queued invocations and tries to invoke each of them.
/// In case of large queues it may be very slow, use [`FIFOInvoker`] instead.
#[derive(Default)]
pub struct NaiveInvoker {
    queue: Vec<InvokerQueueItem>,
}

impl NaiveInvoker {
    /// Creates new NaiveInvoker.
    pub fn new() -> Self {
        Default::default()
    }
}

impl Invoker for NaiveInvoker {
    fn dequeue(
        &mut self,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        stats: &mut Stats,
        time: f64,
    ) -> Vec<DequeuedInvocation> {
        if self.queue.is_empty() {
            return Vec::new();
        }
        let mut new_queue = Vec::new();
        let mut dequeued = Vec::new();
        for item in self.queue.drain(..) {
            let fr_ref = fr.borrow();
            let app = fr_ref.get_app(item.app_id).unwrap();
            let decision = try_invoke(app, cm, time);
            let concurrency_limit = app.get_concurrent_invocations();
            drop(fr_ref);
            match decision {
                InvokerDecision::Warm(id) => {
                    stats.update_queueing_time(item.app_id, item.func_id, time - item.time);
                    let container = cm.get_container_mut(id).unwrap();
                    if container.status == ContainerStatus::Idle {
                        let delta = time - container.last_change;
                        stats.update_wasted_resources(delta, &container.resources);
                    }
                    stats.on_cold_start(item.app_id, item.func_id, time - item.time);
                    container.last_change = time;
                    container.status = ContainerStatus::Running;
                    container.start_invocation(item.invocation_id);
                    if container.invocations.len() == concurrency_limit {
                        cm.move_container_to_full(id);
                    }
                    dequeued.push(DequeuedInvocation::new(item.invocation_id, id, None));
                }
                InvokerDecision::Cold((id, delay)) => {
                    stats.update_queueing_time(item.app_id, item.func_id, time - item.time);
                    cm.reserve_container(id, item.invocation_id);
                    if cm.count_reservations(id) == concurrency_limit {
                        cm.move_container_to_full(id);
                    }
                    stats.on_cold_start(item.app_id, item.func_id, time - item.time + delay);
                    dequeued.push(DequeuedInvocation::new(item.invocation_id, id, Some(delay)));
                }
                InvokerDecision::Rejected => {
                    new_queue.push(item);
                }
                _ => {
                    panic!("try_invoke should only return Warm, Cold or Rejected");
                }
            }
        }
        self.queue = new_queue;
        dequeued
    }

    fn invoke(
        &mut self,
        invocation: &Invocation,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvokerDecision {
        let fr_ref = fr.borrow();
        let app = fr_ref.get_app(invocation.app_id).unwrap();
        let decision = try_invoke(app, cm, time);
        if decision == InvokerDecision::Rejected {
            self.queue.push(InvokerQueueItem::new(
                invocation.id,
                invocation.func_id,
                invocation.app_id,
                invocation.arrival_time,
            ));
            return InvokerDecision::Queued;
        }
        decision
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }

    fn to_string(&self) -> String {
        "NaiveInvoker".to_string()
    }
}

/// `FIFOInvoker` repeatedly tries to invoke the oldest queued invocation.
#[derive(Default)]
pub struct FIFOInvoker {
    queue: VecDeque<InvokerQueueItem>,
}

impl FIFOInvoker {
    /// Creates new FIFOInvoker.
    pub fn new() -> Self {
        Default::default()
    }
}

impl Invoker for FIFOInvoker {
    fn dequeue(
        &mut self,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        stats: &mut Stats,
        time: f64,
    ) -> Vec<DequeuedInvocation> {
        let mut dequeued = Vec::new();
        while let Some(item) = self.queue.front().copied() {
            let fr_ref = fr.borrow();
            let app = fr_ref.get_app(item.app_id).unwrap();
            let status = try_invoke(app, cm, time);
            match status {
                InvokerDecision::Warm(id) => {
                    stats.update_queueing_time(item.app_id, item.func_id, time - item.time);
                    let container = cm.get_container_mut(id).unwrap();
                    if container.status == ContainerStatus::Idle {
                        let delta = time - container.last_change;
                        stats.update_wasted_resources(delta, &container.resources);
                    }
                    stats.on_cold_start(item.app_id, item.func_id, time - item.time);
                    container.last_change = time;
                    container.status = ContainerStatus::Running;
                    container.start_invocation(item.invocation_id);
                    if container.invocations.len() == app.get_concurrent_invocations() {
                        cm.move_container_to_full(id);
                    }
                    dequeued.push(DequeuedInvocation::new(item.invocation_id, id, None));
                    self.queue.pop_front();
                }
                InvokerDecision::Cold((id, delay)) => {
                    stats.update_queueing_time(item.app_id, item.func_id, time - item.time);
                    cm.reserve_container(id, item.invocation_id);
                    if cm.count_reservations(id) == app.get_concurrent_invocations() {
                        cm.move_container_to_full(id);
                    }
                    stats.on_cold_start(item.app_id, item.func_id, time - item.time + delay);
                    dequeued.push(DequeuedInvocation::new(item.invocation_id, id, Some(delay)));
                    self.queue.pop_front();
                }
                InvokerDecision::Rejected => {
                    break;
                }
                _ => {
                    panic!("try_invoke should only return Warm, Cold or Rejected");
                }
            }
        }
        dequeued
    }

    fn invoke(
        &mut self,
        invocation: &Invocation,
        fr: Rc<RefCell<FunctionRegistry>>,
        cm: &mut ContainerManager,
        time: f64,
    ) -> InvokerDecision {
        let fr_ref = fr.borrow();
        let app = fr_ref.get_app(invocation.app_id).unwrap();
        let status = try_invoke(app, cm, time);
        if status == InvokerDecision::Rejected {
            self.queue.push_back(InvokerQueueItem::new(
                invocation.id,
                invocation.func_id,
                invocation.app_id,
                invocation.arrival_time,
            ));
            return InvokerDecision::Queued;
        }
        status
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }

    fn to_string(&self) -> String {
        "FIFOInvoker".to_string()
    }
}

/// Creates [`Invoker`] from a string containing its name and parameters.
pub fn default_invoker_resolver(s: &str) -> Box<dyn Invoker> {
    if s == "NaiveInvoker" {
        Box::new(NaiveInvoker::new())
    } else if s == "FIFOInvoker" {
        Box::new(FIFOInvoker::new())
    } else {
        panic!("Can't resolve: {}", s);
    }
}
