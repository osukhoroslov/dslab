//! Container model and host container manager.

use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::context::SimulationContext;
use dslab_core::event::EventId;

use crate::event::ContainerStartEvent;
use crate::function::Application;
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::util::{Counter, DefaultVecMap, FxIndexMap, FxIndexSet};

#[derive(Eq, PartialEq)]
pub enum ContainerStatus {
    Deploying,
    Running,
    Idle,
    Terminated, // terminated containers may remain in the system due to several events with delay 0.0
}

pub struct Container {
    pub status: ContainerStatus,
    pub id: usize,
    pub host_id: usize,
    pub deployment_time: f64,
    pub app_id: usize,
    pub invocations: FxIndexSet<usize>,
    pub resources: ResourceConsumer,
    pub started_invocations: usize,
    pub last_change: f64,
    pub end_event: Option<EventId>,
    pub cpu_share: f64,
}

impl Container {
    pub fn start_invocation(&mut self, id: usize) {
        self.invocations.insert(id);
        self.started_invocations += 1;
    }

    pub fn end_invocation(&mut self, id: usize, curr_time: f64) {
        self.last_change = curr_time;
        self.invocations.remove(&id);
        if self.invocations.is_empty() {
            self.status = ContainerStatus::Idle;
        }
    }
}

/// Manages container pool of a single host.
pub struct ContainerManager {
    active_invocations: usize,
    host_id: usize,
    resources: ResourceProvider,
    /// A map of containers by id.
    containers: FxIndexMap<usize, Container>,
    /// A set of containers for each app that can accommodate one more invocation.
    free_containers_by_app: DefaultVecMap<FxIndexSet<usize>>,
    /// A set of containers for each app that can't accommodate any more invocations.
    full_containers_by_app: DefaultVecMap<FxIndexSet<usize>>,
    container_counter: Counter,
    /// A set of invocations that will start on each non-running container that is being deployed.
    reservations: FxIndexMap<usize, Vec<usize>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl ContainerManager {
    pub fn new(host_id: usize, resources: ResourceProvider, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        Self {
            active_invocations: 0,
            host_id,
            resources,
            containers: FxIndexMap::default(),
            free_containers_by_app: Default::default(),
            full_containers_by_app: Default::default(),
            container_counter: Counter::default(),
            reservations: FxIndexMap::default(),
            ctx,
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.resources.can_allocate(resources)
    }

    pub fn get_total_resource(&self, id: usize) -> u64 {
        self.resources.get_resource(id).unwrap().get_available()
    }

    pub fn dec_active_invocations(&mut self) {
        self.active_invocations -= 1;
    }

    pub fn inc_active_invocations(&mut self) {
        self.active_invocations += 1;
    }

    pub fn active_invocation_count(&self) -> usize {
        self.active_invocations
    }

    pub fn get_container(&self, id: usize) -> Option<&Container> {
        self.containers.get(&id)
    }

    pub fn get_container_mut(&mut self, id: usize) -> Option<&mut Container> {
        self.containers.get_mut(&id)
    }

    pub fn get_containers(&mut self) -> &mut FxIndexMap<usize, Container> {
        &mut self.containers
    }

    /// Returns an iterator over running containers that can accommodate one more invocation of given app.
    /// If `allow_deploying` is true, also returns containers that are being deployed.
    pub fn get_possible_containers(&self, app: &Application, allow_deploying: bool) -> PossibleContainerIterator<'_> {
        let limit = app.get_concurrent_invocations();
        if let Some(set) = self.free_containers_by_app.get(app.id) {
            return PossibleContainerIterator::new(
                Some(set.iter()),
                &self.containers,
                &self.reservations,
                limit,
                allow_deploying,
            );
        }
        PossibleContainerIterator::new(None, &self.containers, &self.reservations, limit, allow_deploying)
    }

    /// Tries to deploy a new container for given app.
    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(usize, f64)> {
        if self.resources.can_allocate(app.get_resources()) {
            let id = self.deploy_container(app, time);
            return Some((id, app.get_deployment_time()));
        }
        None
    }

    /// Reserves a deploying container for a new invocation.
    pub fn reserve_container(&mut self, id: usize, request: usize) {
        self.reservations.entry(id).or_default().push(request);
    }

    /// Counts reserved invocations for a deploying container.
    pub fn count_reservations(&self, id: usize) -> usize {
        self.reservations.get(&id).map(|x| x.len()).unwrap_or_default()
    }

    pub fn take_reservations(&mut self, id: usize) -> Option<Vec<usize>> {
        self.reservations.remove(&id)
    }

    pub fn delete_container(&mut self, id: usize) {
        let container = self.containers.remove(&id).unwrap();
        self.free_containers_by_app.get_mut(container.app_id).remove(&id);
        self.full_containers_by_app.get_mut(container.app_id).remove(&id);
        self.resources.release(&container.resources);
    }

    /// Moves a container to free if it was full.
    pub fn try_move_container_to_free(&mut self, id: usize) {
        let app_id = self.containers.get(&id).unwrap().app_id;
        let was = self.full_containers_by_app.get_mut(app_id).remove(&id);
        if was {
            self.free_containers_by_app.get_mut(app_id).insert(id);
        }
    }

    pub fn move_container_to_full(&mut self, id: usize) {
        let app_id = self.containers.get(&id).unwrap().app_id;
        let was = self.free_containers_by_app.get_mut(app_id).remove(&id);
        assert!(was);
        self.full_containers_by_app.get_mut(app_id).insert(id);
    }

    /// Deploys a new container for given application.
    fn deploy_container(&mut self, app: &Application, time: f64) -> usize {
        let cont_id = self.container_counter.increment();
        let container = Container {
            status: ContainerStatus::Deploying,
            id: cont_id,
            host_id: self.host_id,
            deployment_time: app.get_deployment_time(),
            app_id: app.id,
            invocations: Default::default(),
            resources: app.get_resources().clone(),
            started_invocations: 0,
            end_event: None,
            last_change: time,
            cpu_share: app.get_cpu_share(),
        };
        self.resources.allocate(&container.resources);
        self.containers.insert(cont_id, container);
        self.free_containers_by_app.get_mut(app.id).insert(cont_id);
        self.ctx
            .borrow_mut()
            .emit_self(ContainerStartEvent { id: cont_id }, app.get_deployment_time());
        cont_id
    }
}

pub struct PossibleContainerIterator<'a> {
    inner: Option<indexmap::set::Iter<'a, usize>>,
    containers: &'a FxIndexMap<usize, Container>,
    reserve: &'a FxIndexMap<usize, Vec<usize>>,
    limit: usize,
    allow_deploying: bool,
}

impl<'a> PossibleContainerIterator<'a> {
    pub fn new(
        inner: Option<indexmap::set::Iter<'a, usize>>,
        containers: &'a FxIndexMap<usize, Container>,
        reserve: &'a FxIndexMap<usize, Vec<usize>>,
        limit: usize,
        allow_deploying: bool,
    ) -> Self {
        Self {
            inner,
            containers,
            reserve,
            limit,
            allow_deploying,
        }
    }
}

impl<'a> Iterator for PossibleContainerIterator<'a> {
    type Item = &'a Container;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = self.inner.as_mut() {
            for id in inner.by_ref() {
                let c = self.containers.get(id).unwrap();
                if c.status != ContainerStatus::Deploying {
                    assert!(c.invocations.len() < self.limit);
                    return Some(c);
                }
                if c.status == ContainerStatus::Deploying && self.allow_deploying {
                    assert!(!self.reserve.contains_key(id) || self.reserve.get(id).unwrap().len() < self.limit);
                    return Some(c);
                }
            }
            return None;
        }
        None
    }
}
