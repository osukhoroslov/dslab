use std::collections::HashMap;

use serde::Serialize;

use crate::util::Counter;

#[derive(Copy, Clone, Debug, Serialize)]
pub struct InvocationRequest {
    pub id: u64,
    pub duration: f64,
    pub time: f64,
}

#[derive(Copy, Clone)]
pub struct Invocation {
    pub request: InvocationRequest,
    pub host_id: u64,
    pub container_id: u64,
    pub finished: Option<f64>,
}

#[derive(Default)]
pub struct InvocationRegistry {
    invocation_ctr: Counter,
    invocations: HashMap<u64, Invocation>,
}

impl InvocationRegistry {
    pub fn new_invocation(&mut self, request: InvocationRequest, host_id: u64, container_id: u64) -> u64 {
        let id = self.invocation_ctr.next();
        let invocation = Invocation {
            request,
            host_id,
            container_id,
            finished: None,
        };
        self.invocations.insert(id, invocation);
        id
    }

    pub fn get_invocation(&self, id: u64) -> Option<&Invocation> {
        self.invocations.get(&id)
    }

    pub fn get_invocation_mut(&mut self, id: u64) -> Option<&mut Invocation> {
        self.invocations.get_mut(&id)
    }
}
