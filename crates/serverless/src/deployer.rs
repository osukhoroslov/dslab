use core::event::Event;
use core::handler::EventHandler;

use crate::container::{Container, ContainerStatus};
use crate::simulation::{CorePtr, ServerlessHandler};

use std::rc::Weak;

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
pub trait Deployer: ServerlessHandler {
    fn deploy(&mut self, id: u64) -> DeploymentResult;
}

pub struct BasicDeployer {
    sim: CorePtr,
}

impl BasicDeployer {
    pub fn new(sim: CorePtr) -> Self {
        Self { sim }
    }
}

impl Deployer for BasicDeployer {
    fn deploy(&mut self, id: u64) -> DeploymentResult {
        let rc = Weak::upgrade(&self.sim).unwrap();
        let mut sim = rc.borrow_mut();
        let mut it = sim.host_mgr.get_hosts();
        if let Some(h) = it.next() {
            let host_id = h.id;
            let delay = sim.function_mgr.get_function(id).unwrap().get_deployment_time();
            let id = sim.container_mgr.new_container(id, delay, ContainerStatus::Deploying);
            sim.new_deploy_event(id, delay);
            sim.host_mgr.get_host_mut(host_id).unwrap().new_container(id);
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

impl EventHandler for BasicDeployer {
    fn on(&mut self, event: Event) {
    }
}

impl ServerlessHandler for BasicDeployer {
    fn register(&mut self, sim: CorePtr) {
        self.sim = sim;
    }
}
