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
use crate::invocation::{InvocationRegistry, InvocationRequest};
use crate::invoker::{InvocationStatus, Invoker};
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::simulation::HandlerId;
use crate::stats::Stats;

pub struct Host {
    id: u64,
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
    pub fn new(
        id: u64,
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

    pub fn get_active_invocations(&self) -> u64 {
        self.container_manager.get_active_invocations()
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

    pub fn invoke(&mut self, request: InvocationRequest, time: f64) -> InvocationStatus {
        self.container_manager.inc_active_invocations();
        let status = self.invoker.invoke(
            request,
            self.function_registry.clone(),
            &mut self.container_manager,
            time,
        );
        let mut stats = self.stats.borrow_mut();
        stats.invocations += 1;
        match status {
            InvocationStatus::Warm(id) => {
                drop(stats);
                self.start_invocation(id, request, time);
            }
            InvocationStatus::Cold((id, delay)) => {
                stats.cold_start_latency.add(delay);
                stats.cold_starts += 1;
                drop(stats);
                self.container_manager.reserve_container(id, request);
            }
            _ => {}
        }
        status
    }

    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(u64, f64)> {
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

    fn start_invocation(&mut self, cont_id: u64, request: InvocationRequest, time: f64) {
        self.invocation_registry
            .borrow_mut()
            .new_invocation(request, self.id, cont_id, time);
        let stats = self.stats.clone();
        let container = self.container_manager.get_container_mut(cont_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let delta = time - container.last_change;
            stats.borrow_mut().update_wasted_resources(delta, &container.resources);
        }
        container.last_change = time;
        container.status = ContainerStatus::Running;
        container.start_invocation(request.id);
        let mut ir = self.invocation_registry.borrow_mut();
        let invocation = ir.get_invocation_mut(request.id).unwrap();
        self.cpu.on_new_invocation(invocation, container, time);
    }

    fn on_container_start(&mut self, id: u64, time: f64) {
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

    pub fn on_container_end(&mut self, id: u64, expected: u64, time: f64) {
        if let Some(cont) = self.container_manager.get_container(id) {
            if cont.status == ContainerStatus::Idle && cont.started_invocations == expected {
                let delta = time - cont.last_change;
                self.stats.borrow_mut().update_wasted_resources(delta, &cont.resources);
                self.container_manager.delete_container(id);
            }
        }
    }

    pub fn on_invocation_end(&mut self, id: u64, time: f64) {
        let ir = self.invocation_registry.clone();
        let fr = self.function_registry.clone();
        let mut invocation_registry = ir.borrow_mut();
        let function_registry = fr.borrow();
        invocation_registry.get_invocation_mut(id).unwrap().finished = Some(time);
        let invocation = invocation_registry.get_invocation_mut(id).unwrap();
        let func_id = invocation.request.func_id;
        let cont_id = invocation.container_id;
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

    fn new_container_end_event(&mut self, container_id: u64, expected: u64, delay: f64) {
        self.ctx.borrow_mut().emit_self(
            ContainerEndEvent {
                id: container_id,
                expected_count: expected,
            },
            delay,
        );
    }
}

impl EventHandler for Host {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            ContainerStartEvent { id } => {
                self.on_container_start(id, event.time);
                self.invoker
                    .dequeue(self.function_registry.clone(), &mut self.container_manager, event.time);
            }
            ContainerEndEvent { id, expected_count } => {
                self.on_container_end(id, expected_count, event.time);
            }
            InvocationEndEvent { id } => {
                self.on_invocation_end(id, event.time);
                self.invoker
                    .dequeue(self.function_registry.clone(), &mut self.container_manager, event.time);
            }
        });
    }
}
