use std::collections::HashSet;

use crate::resource::ResourceConsumer;

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
