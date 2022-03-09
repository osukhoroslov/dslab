use crate::container::ContainerStatus;
use crate::simulation::{Backend, ServerlessContext};

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
 * Deployer chooses a host to deploy container on
 * and triggers deployment event
 */
pub trait Deployer {
    fn deploy(&mut self, id: u64) -> DeploymentResult;
}

pub struct BasicDeployer {
    backend: Rc<RefCell<Backend>>,
    ctx: Rc<RefCell<ServerlessContext>>,
}

impl BasicDeployer {
    pub fn new(backend: Rc<RefCell<Backend>>, ctx: Rc<RefCell<ServerlessContext>>) -> Self {
        Self { backend, ctx }
    }
}

impl Deployer for BasicDeployer {
    fn deploy(&mut self, id: u64) -> DeploymentResult {
        let mut backend = self.backend.borrow_mut();
        let mut it = backend.host_mgr.get_hosts();
        if let Some(h) = it.next() {
            let host_id = h.id;
            let delay = backend.function_mgr.get_function(id).unwrap().get_deployment_time();
            let id = backend
                .container_mgr
                .new_container(id, delay, host_id, ContainerStatus::Deploying);
            backend.host_mgr.get_host_mut(host_id).unwrap().new_container(id);
            self.ctx.borrow_mut().new_deploy_event(id, delay);
            DeploymentResult {
                status: DeploymentStatus::Succeeded,
                container_id: id,
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
