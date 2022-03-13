use crate::container::ContainerStatus;
use crate::invoker::InvocationRequest;
use crate::simulation::{Backend, ServerlessContext};
use crate::stats::Stats;

use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Eq, PartialEq)]
pub enum DeploymentStatus {
    Succeeded,
    Rejected,
}

pub struct DeploymentResult {
    pub status: DeploymentStatus,
    pub container_id: u64,
    pub deployment_time: f64,
}

/*
 * DeployerCore chooses a host to deploy container on
 * and triggers deployment event
 */
pub trait DeployerCore {
    fn deploy(
        &mut self,
        id: u64,
        backend: Rc<RefCell<Backend>>,
        ctx: Rc<RefCell<ServerlessContext>>,
        invocation: Option<InvocationRequest>,
        curr_time: f64,
    ) -> DeploymentResult;
}

pub struct Deployer {
    backend: Rc<RefCell<Backend>>,
    core: Box<dyn DeployerCore>,
    ctx: Rc<RefCell<ServerlessContext>>,
    stats: Rc<RefCell<Stats>>,
}

impl Deployer {
    pub fn new(
        backend: Rc<RefCell<Backend>>,
        core: Box<dyn DeployerCore>,
        ctx: Rc<RefCell<ServerlessContext>>,
        stats: Rc<RefCell<Stats>>,
    ) -> Self {
        Self {
            backend,
            core,
            ctx,
            stats,
        }
    }

    pub fn deploy(&mut self, id: u64, invocation: Option<InvocationRequest>, curr_time: f64) -> DeploymentResult {
        self.core
            .deploy(id, self.backend.clone(), self.ctx.clone(), invocation, curr_time)
    }
}

// BasicDeployer deploys new container on
// the first host with enough resources
pub struct BasicDeployer {}

impl DeployerCore for BasicDeployer {
    fn deploy(
        &mut self,
        id: u64,
        backend: Rc<RefCell<Backend>>,
        ctx: Rc<RefCell<ServerlessContext>>,
        invocation: Option<InvocationRequest>,
        curr_time: f64,
    ) -> DeploymentResult {
        let mut backend_ = backend.borrow_mut();
        let resources = backend_.function_mgr.get_function(id).unwrap().get_resources().clone();
        let mut it = backend_.host_mgr.get_possible_hosts(&resources);
        if let Some(h) = it.next() {
            let host_id = h.id;
            let delay = backend_.function_mgr.get_function(id).unwrap().get_deployment_time();
            let cont = backend_.new_container(id, delay, host_id, ContainerStatus::Deploying, resources, curr_time);
            ctx.borrow_mut().new_deploy_event(cont.id, delay, invocation);
            DeploymentResult {
                status: DeploymentStatus::Succeeded,
                container_id: cont.id,
                deployment_time: delay,
            }
        } else {
            DeploymentResult {
                status: DeploymentStatus::Rejected,
                container_id: u64::MAX,
                deployment_time: 0.,
            }
        }
    }
}
