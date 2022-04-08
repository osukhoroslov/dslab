use crate::resource::ResourceConsumer;
use crate::util::Counter;

use std::collections::HashSet;

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
    pub group_id: u64,
    pub invocations: HashSet<u64>,
    pub resources: ResourceConsumer,
    pub started_invocations: Counter,
    pub last_change: f64,
}

impl Container {
    pub fn end_invocation(&mut self, id: u64, curr_time: f64) {
        self.last_change = curr_time;
        self.invocations.remove(&id);
        if self.invocations.is_empty() {
            self.status = ContainerStatus::Idle;
        }
    }

    pub fn start_invocation(&mut self, id: u64) {
        self.invocations.insert(id);
        self.started_invocations.next();
    }
}
