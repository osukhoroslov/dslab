//! Host model.
//!
//! In DSLab FaaS, the main components of a host are:
//! - [Container manager][crate::container::ContainerManager] -- a component that manages running containers.
//! - [CPU model][crate::cpu::Cpu] -- a component that models CPU sharing among running containers.
//! - [Invoker][crate::invoker::Invoker] -- a component that routes invocation requests to appropriate containers and creates
//! new containers if needed.
use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::coldstart::{ColdStartPolicy, KeepaliveDecision};
use crate::container::{ContainerManager, ContainerStatus};
use crate::cpu::{Cpu, CpuPolicy};
use crate::event::{ContainerEndEvent, ContainerStartEvent, IdleDeployEvent, InvocationEndEvent};
use crate::function::{Application, FunctionRegistry};
use crate::invocation::{InvocationRegistry, InvocationStatus};
use crate::invoker::{Invoker, InvokerDecision};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::simulation::HandlerId;
use crate::stats::Stats;

/// Host model.
pub struct Host {
    id: usize,
    invoker: Box<dyn Invoker>,
    container_manager: ContainerManager,
    cpu: Cpu,
    function_registry: Rc<RefCell<FunctionRegistry>>,
    invocation_registry: Rc<RefCell<InvocationRegistry>>,
    coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    controller_id: HandlerId,
    stats: Rc<RefCell<Stats>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl Host {
    #[allow(clippy::too_many_arguments)]
    /// Creates new host.
    pub fn new(
        id: usize,
        cores: u32,
        cpu_policy: Box<dyn CpuPolicy>,
        resources: ResourceProvider,
        invoker: Box<dyn Invoker>,
        function_registry: Rc<RefCell<FunctionRegistry>>,
        invocation_registry: Rc<RefCell<InvocationRegistry>>,
        coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
        controller_id: HandlerId,
        stats: Rc<RefCell<Stats>>,
        ctx: SimulationContext,
    ) -> Self {
        let ctx = Rc::new(RefCell::new(ctx));
        Self {
            id,
            invoker,
            container_manager: ContainerManager::new(id, resources, ctx.clone()),
            cpu: Cpu::new(cores, cpu_policy, ctx.clone()),
            function_registry,
            invocation_registry,
            coldstart,
            controller_id,
            stats,
            ctx,
        }
    }

    /// Checks whether the host can allocate given resources.
    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.container_manager.can_allocate(resources)
    }

    /// Checks whether the host can invoke a function of the given [`crate::function::Application`] on existing container and optionally on deploying.
    pub fn can_invoke(&self, app: &Application, allow_deploying: bool) -> bool {
        self.container_manager
            .get_possible_containers(app, allow_deploying)
            .next()
            .is_some()
    }

    /// Returns the amount of active invocations on this host.
    pub fn active_invocation_count(&self) -> usize {
        self.container_manager.active_invocation_count()
    }

    /// Returns the amount of queued invocations on this host.
    pub fn queued_invocation_count(&self) -> usize {
        self.invoker.queue_len()
    }

    /// Returns the amount of all existing (active + queued) invocations on this host.
    pub fn total_invocation_count(&self) -> usize {
        self.active_invocation_count() + self.queued_invocation_count()
    }

    /// Returns the total amount of a resource.
    pub fn get_total_resource(&self, id: usize) -> u64 {
        self.container_manager.get_total_resource(id)
    }

    /// Returns the number of CPU cores.
    pub fn get_cpu_cores(&self) -> u32 {
        self.cpu.cores
    }

    /// Returns current CPU load.
    pub fn get_cpu_load(&self) -> f64 {
        self.cpu.get_load()
    }

    /// Passes an invocation to the [`crate::invoker::Invoker`], which either assigns it to a container or puts it in queue.
    pub fn invoke(&mut self, id: usize, time: f64) -> InvokerDecision {
        let mut ir = self.invocation_registry.borrow_mut();
        let invocation = &mut ir[id];
        invocation.host_id = Some(self.id);
        self.container_manager.inc_active_invocations();
        let concurrency_limit = self
            .function_registry
            .borrow()
            .get_app(invocation.app_id)
            .unwrap()
            .get_concurrent_invocations();
        let status = self.invoker.invoke(
            invocation,
            self.function_registry.clone(),
            &mut self.container_manager,
            time,
        );
        let mut stats = self.stats.borrow_mut();
        stats.on_new_invocation(invocation.app_id, invocation.func_id);
        match status {
            InvokerDecision::Warm(container_id) => {
                drop(stats);
                drop(ir);
                self.start_invocation(container_id, id, time);
                if self
                    .container_manager
                    .get_container(container_id)
                    .unwrap()
                    .invocations
                    .len()
                    == concurrency_limit
                {
                    self.container_manager.move_container_to_full(container_id);
                }
            }
            InvokerDecision::Cold((container_id, delay)) => {
                invocation.status = InvocationStatus::WaitingForContainer;
                invocation.container_id = Some(container_id);
                stats.on_cold_start(invocation.app_id, invocation.func_id, delay);
                drop(stats);
                self.container_manager.reserve_container(container_id, id);
                if self.container_manager.count_reservations(container_id) == concurrency_limit {
                    self.container_manager.move_container_to_full(container_id);
                }
            }
            _ => {
                invocation.status = InvocationStatus::Queued;
            }
        }
        status
    }

    /// Tries to deploy a new container for the given application.
    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(usize, f64)> {
        self.container_manager.try_deploy(app, time)
    }

    /// Updates wasted resources for idle containers.
    pub fn update_end_metrics(&mut self, time: f64) {
        let mut stats = self.stats.borrow_mut();
        for (_, container) in self.container_manager.get_containers().iter_mut() {
            if container.status == ContainerStatus::Idle {
                let delta = time - container.last_change;
                stats.update_wasted_resources(delta, &container.resources);
                container.last_change = time;
            }
        }
    }

    /// Starts an invocation.
    fn start_invocation(&mut self, cont_id: usize, id: usize, time: f64) {
        let container = self.container_manager.get_container_mut(cont_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let delta = time - container.last_change;
            self.stats
                .borrow_mut()
                .update_wasted_resources(delta, &container.resources);
        }
        container.last_change = time;
        container.status = ContainerStatus::Running;
        container.start_invocation(id);
        let mut ir = self.invocation_registry.borrow_mut();
        let invocation = &mut ir[id];
        invocation.start_time = Some(time);
        invocation.status = InvocationStatus::Running;
        invocation.container_id = Some(cont_id);
        self.cpu.on_new_invocation(invocation, container, time);
    }

    fn on_container_start(&mut self, id: usize, time: f64) {
        if let Some(invocations) = self.container_manager.take_reservations(id) {
            for invocation in invocations {
                self.start_invocation(id, invocation, time);
            }
        } else {
            let container = self.container_manager.get_container_mut(id).unwrap();
            container.last_change = time;
            container.status = ContainerStatus::Idle;
            let immut_container = self.container_manager.get_container(id).unwrap();
            let decision = self.coldstart.borrow_mut().keepalive_decision(immut_container);
            match decision {
                KeepaliveDecision::NewWindow(keepalive) => {
                    self.new_container_end_event(id, keepalive);
                }
                KeepaliveDecision::OldWindow => {
                    panic!("Keepalive policy returned OldWindow for newly created container!");
                }
                KeepaliveDecision::TerminateNow => {
                    // weird but ok
                    self.new_container_end_event(id, 0.0);
                }
            }
        }
    }

    fn on_container_end(&mut self, id: usize, time: f64) {
        if let Some(cont) = self.container_manager.get_container(id) {
            if cont.status == ContainerStatus::Idle || cont.status == ContainerStatus::Terminated {
                let delta = time - cont.last_change;
                self.stats.borrow_mut().update_wasted_resources(delta, &cont.resources);
                self.container_manager.delete_container(id);
            }
        }
    }

    fn on_invocation_end(&mut self, id: usize, time: f64) {
        let ir = self.invocation_registry.clone();
        let fr = self.function_registry.clone();
        let mut invocation_registry = ir.borrow_mut();
        let function_registry = fr.borrow();
        let invocation = &mut invocation_registry[id];
        invocation.finish_time = Some(time);
        invocation.status = InvocationStatus::Finished;
        let func_id = invocation.func_id;
        let cont_id = invocation.container_id.unwrap();
        let app_id = function_registry.get_function(func_id).unwrap().app_id;
        self.coldstart
            .borrow_mut()
            .update(invocation, self.function_registry.borrow().get_app(app_id).unwrap());
        self.container_manager.dec_active_invocations();
        self.container_manager.try_move_container_to_free(cont_id);
        let container = self.container_manager.get_container_mut(cont_id).unwrap();
        container.end_invocation(id, time);
        self.stats.borrow_mut().update_invocation_stats(invocation);
        self.cpu.on_invocation_end(invocation, container, time);
        let app = function_registry.get_app(app_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let prewarm = f64::max(0.0, self.coldstart.borrow_mut().prewarm_window(app));
            if prewarm > 1e-9 {
                let expected_invocation = self.stats.borrow().app_stats.get(app_id).unwrap().invocations;
                let ctx = self.ctx.borrow_mut();
                ctx.emit(
                    IdleDeployEvent {
                        id: app_id,
                        expected_invocation,
                    },
                    self.controller_id,
                    prewarm,
                );
                if let Some(id) = container.end_event {
                    ctx.cancel_event(id);
                }
                drop(ctx);
                self.new_container_end_event(cont_id, 0.0);
            } else {
                let immut_container = self.container_manager.get_container(cont_id).unwrap();
                let decision = self.coldstart.borrow_mut().keepalive_decision(immut_container);
                match decision {
                    KeepaliveDecision::NewWindow(keepalive) => {
                        if let Some(id) = immut_container.end_event {
                            self.ctx.borrow_mut().cancel_event(id);
                        }
                        self.new_container_end_event(cont_id, f64::max(0.0, keepalive));
                    }
                    KeepaliveDecision::OldWindow => {}
                    KeepaliveDecision::TerminateNow => {
                        if let Some(id) = immut_container.end_event {
                            self.ctx.borrow_mut().cancel_event(id);
                        }
                        self.new_container_end_event(cont_id, 0.0);
                    }
                }
            }
        }
    }

    fn new_container_end_event(&mut self, container_id: usize, delay: f64) {
        let event_id = self
            .ctx
            .borrow_mut()
            .emit_self(ContainerEndEvent { id: container_id }, delay);
        let container = self.container_manager.get_container_mut(container_id).unwrap();
        container.end_event = Some(event_id);
        if delay == 0.0 {
            container.status = ContainerStatus::Terminated;
        }
    }

    fn dequeue_requests(&mut self, time: f64) {
        let mut reqs = self.invoker.dequeue(
            self.function_registry.clone(),
            &mut self.container_manager,
            &mut self.stats.borrow_mut(),
            time,
        );
        if reqs.is_empty() {
            return;
        }
        for req in reqs.drain(..) {
            let mut ir = self.invocation_registry.borrow_mut();
            let invocation = &mut ir[req.id];
            invocation.container_id = Some(req.container_id);
            if req.delay.is_none() {
                invocation.status = InvocationStatus::Running;
                invocation.start_time = Some(time);
                let container = self.container_manager.get_container_mut(req.container_id).unwrap();
                self.cpu.on_new_invocation(invocation, container, time);
            } else {
                invocation.status = InvocationStatus::WaitingForContainer;
            }
        }
    }
}

impl EventHandler for Host {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            ContainerStartEvent { id } => {
                self.on_container_start(id, event.time);
                self.dequeue_requests(event.time);
            }
            ContainerEndEvent { id } => {
                self.on_container_end(id, event.time);
                self.dequeue_requests(event.time);
            }
            InvocationEndEvent { id } => {
                self.on_invocation_end(id, event.time);
                self.dequeue_requests(event.time);
            }
        });
    }
}
