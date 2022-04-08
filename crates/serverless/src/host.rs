use crate::coldstart::ColdStartPolicy;
use crate::container::{Container, ContainerStatus};
use crate::event::{ContainerEndEvent, ContainerStartEvent, IdleDeployEvent, InvocationEndEvent};
use crate::function::{FunctionRegistry, Group};
use crate::invocation::{InvocationRegistry, InvocationRequest};
use crate::invoker::InvocationStatus;
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::simulation::HandlerId;
use crate::stats::Stats;
use crate::util::Counter;

use simcore::context::SimulationContext;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub struct Host {
    id: u64,
    coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
    containers: HashMap<u64, Container>,
    containers_by_group: HashMap<u64, HashSet<u64>>,
    container_counter: Counter,
    controller_handler_id: HandlerId,
    ctx: SimulationContext,
    pub function_registry: Rc<RefCell<FunctionRegistry>>,
    invocation_registry: Rc<RefCell<InvocationRegistry>>,
    pub invoker_handler_id: HandlerId,
    reservations: HashMap<u64, Vec<InvocationRequest>>,
    pub resources: ResourceProvider,
    stats: Rc<RefCell<Stats>>,
}

impl Host {
    pub fn new(
        id: u64,
        coldstart: Rc<RefCell<dyn ColdStartPolicy>>,
        controller_handler_id: HandlerId,
        ctx: SimulationContext,
        function_registry: Rc<RefCell<FunctionRegistry>>,
        invocation_registry: Rc<RefCell<InvocationRegistry>>,
        resources: ResourceProvider,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        Self {
            id,
            coldstart,
            containers: Default::default(),
            containers_by_group: Default::default(),
            container_counter: Default::default(),
            controller_handler_id,
            ctx,
            function_registry,
            invocation_registry,
            invoker_handler_id: 0,
            reservations: Default::default(),
            resources,
            stats,
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.resources.can_allocate(resources)
    }

    pub fn can_invoke(&self, group: &Group) -> bool {
        self.get_possible_containers(group).next().is_some()
    }

    pub fn get_container(&self, id: u64) -> Option<&Container> {
        self.containers.get(&id)
    }

    pub fn get_container_mut(&mut self, id: u64) -> Option<&mut Container> {
        self.containers.get_mut(&id)
    }

    pub fn get_possible_containers(&self, group: &Group) -> PossibleContainerIterator<'_> {
        let id = group.id;
        let limit = group.get_concurrent_invocations();
        if let Some(set) = self.containers_by_group.get(&id) {
            return PossibleContainerIterator::new(Some(set.iter()), &self.containers, &self.reservations, limit);
        }
        PossibleContainerIterator::new(None, &self.containers, &self.reservations, limit)
    }

    pub fn deploy_container(&mut self, group: &Group, time: f64) -> u64 {
        let cont_id = self.container_counter.next();
        let container = Container {
            status: ContainerStatus::Deploying,
            id: cont_id,
            deployment_time: group.get_deployment_time(),
            group_id: group.id,
            invocations: Default::default(),
            resources: group.get_resources().clone(),
            started_invocations: Default::default(),
            last_change: time,
        };
        self.resources.allocate(&container.resources);
        self.containers.insert(cont_id, container);
        if !self.containers_by_group.contains_key(&group.id) {
            self.containers_by_group.insert(group.id, HashSet::new());
        }
        self.containers_by_group.get_mut(&group.id).unwrap().insert(cont_id);
        self.new_container_start_event(cont_id, group.get_deployment_time());
        cont_id
    }

    pub fn delete_container(&mut self, id: u64) {
        let container = self.containers.remove(&id).unwrap();
        self.containers_by_group
            .get_mut(&container.group_id)
            .unwrap()
            .remove(&id);
        self.resources.release(&container.resources);
    }

    pub fn end_container(&mut self, id: u64, expected: u64, time: f64) {
        if let Some(cont) = self.get_container(id) {
            if cont.status == ContainerStatus::Idle && cont.started_invocations.curr() == expected {
                let delta = time - cont.last_change;
                self.stats.borrow_mut().update_wasted_resources(delta, &cont.resources);
                self.delete_container(id);
            }
        }
    }

    pub fn end_invocation(&mut self, id: u64, time: f64) {
        let ir = self.invocation_registry.clone();
        let fr = self.function_registry.clone();
        let mut invocation_registry = ir.borrow_mut();
        let function_registry = fr.borrow();
        invocation_registry.get_invocation_mut(id).unwrap().finished = Some(time);
        let invocation = invocation_registry.get_invocation(id).unwrap();
        let func_id = invocation.request.id;
        let cont_id = invocation.container_id;
        let group_id = function_registry.get_function(func_id).unwrap().group_id;
        self.coldstart
            .borrow_mut()
            .update(invocation, self.function_registry.borrow().get_group(group_id).unwrap());
        let container = self.get_container_mut(cont_id).unwrap();
        container.end_invocation(id, time);
        let expect = container.started_invocations.curr();
        let group = function_registry.get_group(group_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let prewarm = self.coldstart.borrow_mut().prewarm_window(group);
            if prewarm != 0. {
                self.new_idle_deploy_event(group_id, prewarm);
                self.new_container_end_event(cont_id, expect, 0.0);
            } else {
                let immut_container = self.get_container(cont_id).unwrap();
                let keepalive = self.coldstart.borrow_mut().keepalive_window(immut_container);
                self.new_container_end_event(cont_id, expect, keepalive);
            }
        }
    }

    pub fn new_container_start_event(&mut self, container_id: u64, delay: f64) {
        self.ctx
            .emit(ContainerStartEvent { id: container_id }, self.invoker_handler_id, delay);
    }

    pub fn new_container_end_event(&mut self, container_id: u64, expected: u64, delay: f64) {
        self.ctx.emit(
            ContainerEndEvent {
                id: container_id,
                expected_count: expected,
            },
            self.invoker_handler_id,
            delay,
        );
    }

    pub fn new_idle_deploy_event(&mut self, group_id: u64, prewarm: f64) {
        self.ctx
            .emit(IdleDeployEvent { id: group_id }, self.controller_handler_id, prewarm);
    }

    pub fn new_invocation_end_event(&mut self, id: u64, delay: f64) {
        self.ctx.emit(InvocationEndEvent { id }, self.invoker_handler_id, delay);
    }

    pub fn process_response(&mut self, request: InvocationRequest, response: InvocationStatus, time: f64) {
        let mut stats = self.stats.borrow_mut();
        stats.invocations += 1;
        match response {
            InvocationStatus::Warm(id) => {
                drop(stats);
                self.start_invocation(id, request, time);
            }
            InvocationStatus::Cold((id, delay)) => {
                stats.cold_starts_total_time += delay;
                stats.cold_starts += 1;
                drop(stats);
                self.reserve_container(id, request);
            }
            _ => {}
        }
    }

    pub fn take_reservations(&mut self, id: u64) -> Option<Vec<InvocationRequest>> {
        self.reservations.remove(&id)
    }

    pub fn reserve_container(&mut self, id: u64, request: InvocationRequest) {
        if let Some(reserve) = self.reservations.get_mut(&id) {
            reserve.push(request);
        } else {
            self.reservations.insert(id, vec![request]);
        }
    }

    pub fn start_container(&mut self, id: u64, time: f64) {
        let mut invocations = Vec::new();
        if let Some(reserve) = self.take_reservations(id) {
            invocations = reserve;
        }
        let container = self.get_container_mut(id).unwrap();
        if !invocations.is_empty() {
            for invocation in invocations {
                self.start_invocation(id, invocation, time);
            }
        } else {
            container.status = ContainerStatus::Idle;
            let immut_container = self.get_container(id).unwrap();
            let keepalive = self.coldstart.borrow_mut().keepalive_window(immut_container);
            self.new_container_end_event(id, 0, keepalive);
        }
    }

    pub fn start_invocation(&mut self, cont_id: u64, request: InvocationRequest, time: f64) {
        let inv_id = self
            .invocation_registry
            .borrow_mut()
            .new_invocation(request, self.id, cont_id);
        let stats = self.stats.clone();
        let container = self.get_container_mut(cont_id).unwrap();
        if container.status == ContainerStatus::Idle {
            let delta = time - container.last_change;
            stats.borrow_mut().update_wasted_resources(delta, &container.resources);
        }
        container.last_change = time;
        container.status = ContainerStatus::Running;
        container.start_invocation(inv_id);
        self.new_invocation_end_event(inv_id, request.duration);
    }

    pub fn try_deploy(&mut self, group: &Group, time: f64) -> Option<(u64, f64)> {
        if self.resources.can_allocate(group.get_resources()) {
            let id = self.deploy_container(group, time);
            return Some((id, group.get_deployment_time()));
        }
        None
    }

    pub fn update_end_metrics(&mut self, time: f64) {
        let mut stats = self.stats.borrow_mut();
        for (_, container) in self.containers.iter_mut() {
            if container.status == ContainerStatus::Idle {
                let delta = time - container.last_change;
                stats.update_wasted_resources(delta, &container.resources);
                container.status = ContainerStatus::Deploying;
            }
        }
    }
}

pub struct PossibleContainerIterator<'a> {
    inner: Option<std::collections::hash_set::Iter<'a, u64>>,
    containers: &'a HashMap<u64, Container>,
    reserve: &'a HashMap<u64, Vec<InvocationRequest>>,
    limit: usize,
}

impl<'a> PossibleContainerIterator<'a> {
    pub fn new(
        inner: Option<std::collections::hash_set::Iter<'a, u64>>,
        containers: &'a HashMap<u64, Container>,
        reserve: &'a HashMap<u64, Vec<InvocationRequest>>,
        limit: usize,
    ) -> Self {
        Self {
            inner,
            containers,
            reserve,
            limit,
        }
    }
}

impl<'a> Iterator for PossibleContainerIterator<'a> {
    type Item = &'a Container;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = self.inner.as_mut() {
            while let Some(id) = inner.next() {
                let c = self.containers.get(&id).unwrap();
                if c.status != ContainerStatus::Deploying && c.invocations.len() < self.limit {
                    return Some(c);
                }
                if c.status == ContainerStatus::Deploying
                    && (!self.reserve.contains_key(&id) || self.reserve.get(&id).unwrap().len() < self.limit)
                {
                    return Some(c);
                }
            }
            return None;
        }
        None
    }
}
