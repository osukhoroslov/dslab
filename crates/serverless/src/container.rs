use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use simcore::context::SimulationContext;

use crate::event::ContainerStartEvent;
use crate::function::Application;
use crate::invocation::InvocationRequest;
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::util::Counter;

#[derive(Eq, PartialEq)]
pub enum ContainerStatus {
    Deploying,
    Running,
    Idle,
}

pub struct Container {
    pub status: ContainerStatus,
    pub id: u64,
    pub deployment_time: f64,
    pub app_id: u64,
    pub invocations: HashSet<u64>,
    pub resources: ResourceConsumer,
    pub started_invocations: u64,
    pub last_change: f64,
}

impl Container {
    pub fn start_invocation(&mut self, id: u64) {
        self.invocations.insert(id);
        self.started_invocations += 1;
    }

    pub fn end_invocation(&mut self, id: u64, curr_time: f64) {
        self.last_change = curr_time;
        self.invocations.remove(&id);
        if self.invocations.is_empty() {
            self.status = ContainerStatus::Idle;
        }
    }
}

pub struct ContainerManager {
    resources: ResourceProvider,
    containers: HashMap<u64, Container>,
    containers_by_app: HashMap<u64, HashSet<u64>>,
    container_counter: Counter,
    reservations: HashMap<u64, Vec<InvocationRequest>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl ContainerManager {
    pub fn new(resources: ResourceProvider, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        Self {
            resources,
            containers: HashMap::new(),
            containers_by_app: HashMap::new(),
            container_counter: Counter::default(),
            reservations: HashMap::new(),
            ctx,
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.resources.can_allocate(resources)
    }

    pub fn get_container(&self, id: u64) -> Option<&Container> {
        self.containers.get(&id)
    }

    pub fn get_container_mut(&mut self, id: u64) -> Option<&mut Container> {
        self.containers.get_mut(&id)
    }

    pub fn get_containers(&mut self) -> &mut HashMap<u64, Container> {
        &mut self.containers
    }

    pub fn get_possible_containers(&self, app: &Application) -> PossibleContainerIterator<'_> {
        let id = app.id;
        let limit = app.get_concurrent_invocations();
        if let Some(set) = self.containers_by_app.get(&id) {
            return PossibleContainerIterator::new(Some(set.iter()), &self.containers, &self.reservations, limit);
        }
        PossibleContainerIterator::new(None, &self.containers, &self.reservations, limit)
    }

    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(u64, f64)> {
        if self.resources.can_allocate(app.get_resources()) {
            let id = self.deploy_container(app, time);
            return Some((id, app.get_deployment_time()));
        }
        None
    }

    pub fn reserve_container(&mut self, id: u64, request: InvocationRequest) {
        if let Some(reserve) = self.reservations.get_mut(&id) {
            reserve.push(request);
        } else {
            self.reservations.insert(id, vec![request]);
        }
    }

    pub fn take_reservations(&mut self, id: u64) -> Option<Vec<InvocationRequest>> {
        self.reservations.remove(&id)
    }

    pub fn delete_container(&mut self, id: u64) {
        let container = self.containers.remove(&id).unwrap();
        self.containers_by_app.get_mut(&container.app_id).unwrap().remove(&id);
        self.resources.release(&container.resources);
    }

    fn deploy_container(&mut self, app: &Application, time: f64) -> u64 {
        let cont_id = self.container_counter.next();
        let container = Container {
            status: ContainerStatus::Deploying,
            id: cont_id,
            deployment_time: app.get_deployment_time(),
            app_id: app.id,
            invocations: Default::default(),
            resources: app.get_resources().clone(),
            started_invocations: 0u64,
            last_change: time,
        };
        self.resources.allocate(&container.resources);
        self.containers.insert(cont_id, container);
        if !self.containers_by_app.contains_key(&app.id) {
            self.containers_by_app.insert(app.id, HashSet::new());
        }
        self.containers_by_app.get_mut(&app.id).unwrap().insert(cont_id);
        self.ctx
            .borrow_mut()
            .emit_self(ContainerStartEvent { id: cont_id }, app.get_deployment_time());
        cont_id
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
