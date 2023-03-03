use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;

use crate::coldstart::ColdStartPolicy;
use crate::container::{ContainerManager, ContainerStatus};
use crate::cpu::CPU;
use crate::event::{ContainerEndEvent, ContainerStartEvent, IdleDeployEvent, InvocationEndEvent};
use crate::function::{Application, FunctionRegistry};
use crate::invocation::{InvocationRegistry, InvocationStatus};
use crate::invoker::{InvokerDecision, Invoker};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::simulation::HandlerId;
use crate::stats::Stats;

pub struct Host {
    id: usize,
    invoker: Box<dyn Invoker>,
    container_manager: ContainerManager,
    cpu: CPU,
    function_registry: Rc<RefCell<FunctionRegistry>>,
    invocation_registry: Rc<RefCell<InvocationRegistry>>,
    coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    controller_id: HandlerId,
    stats: Rc<RefCell<Stats>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl Host {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        cores: u32,
        disable_contention: bool,
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
            container_manager: ContainerManager::new(resources, ctx.clone()),
            cpu: CPU::new(cores, disable_contention, ctx.clone()),
            function_registry,
            invocation_registry,
            coldstart,
            controller_id,
            stats,
            ctx,
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.container_manager.can_allocate(resources)
    }

    pub fn can_invoke(&self, app: &Application, allow_deploying: bool) -> bool {
        self.container_manager
            .get_possible_containers(app, allow_deploying)
            .next()
            .is_some()
    }

    pub fn active_invocation_count(&self) -> usize {
        self.container_manager.active_invocation_count()
    }

    pub fn queued_invocation_count(&self) -> usize {
        self.invoker.queue_len()
    }

    pub fn total_invocation_count(&self) -> usize {
        self.active_invocation_count() + self.queued_invocation_count()
    }

    pub fn get_total_resource(&self, id: usize) -> u64 {
        self.container_manager.get_total_resource(id)
    }

    pub fn get_cpu_cores(&self) -> u32 {
        self.cpu.cores
    }

    pub fn get_cpu_load(&self) -> f64 {
        self.cpu.get_load()
    }

    pub fn invoke(&mut self, id: usize, time: f64) -> InvokerDecision {
        let mut ir = self.invocation_registry.borrow_mut();
        ir[id].host_id = Some(self.id);
        self.container_manager.inc_active_invocations();
        let status = self.invoker.invoke(
            &ir[id],
            self.function_registry.clone(),
            &mut self.container_manager,
            time,
        );
        let mut stats = self.stats.borrow_mut();
        stats.on_new_invocation(ir[id].func_id);
        match status {
            InvokerDecision::Warm(container_id) => {
                drop(stats);
                drop(ir);
                self.start_invocation(container_id, id, time);
            }
            InvokerDecision::Cold((container_id, delay)) => {
                ir[id].status = InvocationStatus::WaitingForContainer;
                ir[id].container_id = Some(container_id);
                stats.on_cold_start(ir[id].func_id, delay);
                drop(stats);
                self.container_manager.reserve_container(container_id, id);
            }
            _ => {
                ir[id].status = InvocationStatus::Queued;
            }
        }
        status
    }

    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(usize, f64)> {
        self.container_manager.try_deploy(app, time)
    }

    pub fn update_end_metrics(&mut self, time: f64) {
        let mut stats = self.stats.borrow_mut();
        for (_, container) in self.container_manager.get_containers().iter_mut() {
            if container.status == ContainerStatus::Idle {
                let delta = time - container.last_change;
                stats.update_wasted_resources(delta, &container.resources);
                container.status = ContainerStatus::Deploying;
            }
        }
    }

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
        let mut invocation = &mut ir[id];
        invocation.started = Some(time);
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
            container.status = ContainerStatus::Idle;
            let immut_container = self.container_manager.get_container(id).unwrap();
            let keepalive = self.coldstart.borrow_mut().keepalive_window(immut_container);
            self.new_container_end_event(id, 0, keepalive);
        }
    }

    pub fn on_container_end(&mut self, id: usize, expected: usize, time: f64) {
        if let Some(cont) = self.container_manager.get_container(id) {
            if cont.status == ContainerStatus::Idle && cont.started_invocations == expected {
                let delta = time - cont.last_change;
                self.stats.borrow_mut().update_wasted_resources(delta, &cont.resources);
                self.container_manager.delete_container(id);
            }
        }
    }

    pub fn on_invocation_end(&mut self, id: usize, time: f64) {
        let ir = self.invocation_registry.clone();
        let fr = self.function_registry.clone();
        let mut invocation_registry = ir.borrow_mut();
        let function_registry = fr.borrow();
        let mut invocation = &mut invocation_registry[id];
        invocation.finished = Some(time);
        invocation.status = InvocationStatus::Finished;
        let func_id = invocation.func_id;
        let cont_id = invocation.container_id.unwrap();
        let app_id = function_registry.get_function(func_id).unwrap().app_id;
        self.coldstart
            .borrow_mut()
            .update(invocation, self.function_registry.borrow().get_app(app_id).unwrap());
        self.container_manager.dec_active_invocations();
        let container = self.container_manager.get_container_mut(cont_id).unwrap();
        container.end_invocation(id, time);
        self.stats.borrow_mut().update_invocation_stats(invocation);
        self.cpu.on_invocation_end(invocation, container, time);
        let expect = container.started_invocations;
        let app = function_registry.get_app(app_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let prewarm = f64::max(0.0, self.coldstart.borrow_mut().prewarm_window(app));
            if prewarm != 0. {
                self.ctx
                    .borrow_mut()
                    .emit(IdleDeployEvent { id: app_id }, self.controller_id, prewarm);
                self.new_container_end_event(cont_id, expect, 0.0);
            } else {
                let immut_container = self.container_manager.get_container(cont_id).unwrap();
                let keepalive = f64::max(0.0, self.coldstart.borrow_mut().keepalive_window(immut_container));
                self.new_container_end_event(cont_id, expect, keepalive);
            }
        }
    }

    fn new_container_end_event(&mut self, container_id: usize, expected: usize, delay: f64) {
        self.ctx.borrow_mut().emit_self(
            ContainerEndEvent {
                id: container_id,
                expected_count: expected,
            },
            delay,
        );
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
            let mut invocation = &mut ir[req.id];
            invocation.container_id = Some(req.container_id);
            if req.delay.is_none() {
                invocation.status = InvocationStatus::Running;
                invocation.started = Some(time);
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
            ContainerEndEvent { id, expected_count } => {
                self.on_container_end(id, expected_count, event.time);
            }
            InvocationEndEvent { id } => {
                self.on_invocation_end(id, event.time);
                self.dequeue_requests(event.time);
            }
        });
    }
}
