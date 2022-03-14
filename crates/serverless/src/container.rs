use crate::host::HostManager;
use crate::invoker::InvocationRequest;
use crate::resource::ResourceConsumer;
use crate::util::Counter;

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

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
    pub func_id: u64,
    pub invocation: Option<u64>,
    pub finished_invocations: Counter,
    pub host_id: u64,
    pub resources: ResourceConsumer,
    pub last_change: f64,
    pub prewarmed: bool,
}

impl Container {
    pub fn end_invocation(&mut self, curr_time: f64) -> u64 {
        self.last_change = curr_time;
        self.invocation = None;
        self.status = ContainerStatus::Idle;
        self.finished_invocations.next()
    }
}

impl Hash for Container {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Default)]
pub struct ContainerManager {
    container_ctr: Counter,
    containers_by_func: HashMap<u64, HashSet<u64>>,
    containers: HashMap<u64, Container>,
    prewarm_stolen: HashMap<u64, InvocationRequest>,
}

impl ContainerManager {
    pub fn get_possible_containers(&self, id: u64) -> PossibleContainerIterator<'_> {
        if let Some(set) = self.containers_by_func.get(&id) {
            return PossibleContainerIterator::new(Some(set.iter()), &self.containers, &self.prewarm_stolen);
        }
        PossibleContainerIterator::new(None, &self.containers, &self.prewarm_stolen)
    }

    pub fn get_container(&self, id: u64) -> Option<&Container> {
        self.containers.get(&id)
    }

    pub fn get_containers(&self) -> ContainerIterator<'_> {
        ContainerIterator::new(self.containers.iter())
    }

    pub fn get_container_mut(&mut self, id: u64) -> Option<&mut Container> {
        self.containers.get_mut(&id)
    }

    pub fn new_container(
        &mut self,
        host_mgr: &mut HostManager,
        func_id: u64,
        deployment_time: f64,
        host_id: u64,
        status: ContainerStatus,
        resources: ResourceConsumer,
        curr_time: f64,
        prewarmed: bool,
    ) -> &Container {
        let id = self.container_ctr.next();
        if !self.containers_by_func.contains_key(&func_id) {
            self.containers_by_func.insert(func_id, HashSet::new());
        }
        self.containers_by_func.get_mut(&func_id).unwrap().insert(id);
        let container = Container {
            status,
            id,
            deployment_time,
            func_id,
            invocation: None,
            finished_invocations: Default::default(),
            host_id,
            resources,
            last_change: curr_time,
            prewarmed,
        };
        self.containers.insert(id, container);
        let cont_ref = self.containers.get(&id).unwrap();
        host_mgr.get_host_mut(host_id).unwrap().new_container(cont_ref);
        cont_ref
    }

    pub fn destroy_container(&mut self, id: u64) {
        if self.containers.contains_key(&id) {
            let func_id = self.get_container(id).unwrap().func_id;
            self.containers_by_func.get_mut(&func_id).unwrap().remove(&id);
            self.containers.remove(&id);
        }
    }

    pub fn get_stolen_prewarm(&self, id: u64) -> Option<InvocationRequest> {
        self.prewarm_stolen.get(&id).cloned()
    }

    pub fn steal_prewarm(&mut self, id: u64, request: InvocationRequest) {
        self.prewarm_stolen.insert(id, request);
    }
}

pub struct PossibleContainerIterator<'a> {
    inner: Option<std::collections::hash_set::Iter<'a, u64>>,
    containers: &'a HashMap<u64, Container>,
    stolen: &'a HashMap<u64, InvocationRequest>,
}

impl<'a> PossibleContainerIterator<'a> {
    pub fn new(
        inner: Option<std::collections::hash_set::Iter<'a, u64>>,
        containers: &'a HashMap<u64, Container>,
        stolen: &'a HashMap<u64, InvocationRequest>,
    ) -> Self {
        Self {
            inner,
            containers,
            stolen,
        }
    }
}

impl<'a> Iterator for PossibleContainerIterator<'a> {
    type Item = &'a Container;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = self.inner.as_mut() {
            while let Some(id) = inner.next() {
                let c = self.containers.get(&id).unwrap();
                if c.status == ContainerStatus::Idle {
                    return Some(c);
                }
                if c.status == ContainerStatus::Deploying && c.prewarmed && !self.stolen.contains_key(&id) {
                    return Some(c);
                }
            }
            return None;
        }
        None
    }
}

pub struct ContainerIterator<'a> {
    inner: std::collections::hash_map::Iter<'a, u64, Container>,
}

impl<'a> ContainerIterator<'a> {
    pub fn new(inner: std::collections::hash_map::Iter<'a, u64, Container>) -> Self {
        Self { inner }
    }
}

impl<'a> Iterator for ContainerIterator<'a> {
    type Item = &'a Container;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, v)) = self.inner.next() {
            Some(v)
        } else {
            None
        }
    }
}
