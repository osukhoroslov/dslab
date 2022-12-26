use std::collections::HashMap;

use serde::Serialize;

use crate::util::Counter;

#[derive(Copy, Clone, Debug, Serialize)]
pub struct InvocationRequest {
    pub func_id: u64,
    pub duration: f64,
    pub time: f64,
    pub id: u64,
}

#[derive(Copy, Clone)]
pub struct Invocation {
    pub id: u64,
    pub request: InvocationRequest,
    pub host_id: u64,
    pub container_id: u64,
    pub started: f64,
    pub finished: Option<f64>,
}

#[derive(Default)]
pub struct InvocationRegistry {
    invocation_ctr: Counter,
    invocations: HashMap<u64, Invocation>,
}

impl InvocationRegistry {
    pub fn new_invocation(&mut self, request: InvocationRequest, host_id: u64, container_id: u64, time: f64) {
        let id = request.id;
        let invocation = Invocation {
            id,
            request,
            host_id,
            container_id,
            started: time,
            finished: None,
        };
        self.invocations.insert(id, invocation);
    }

    pub fn register_invocation(&mut self) -> u64 {
        self.invocation_ctr.increment()
    }

    pub fn get_invocation(&self, id: u64) -> Option<&Invocation> {
        self.invocations.get(&id)
    }

    pub fn get_invocation_mut(&mut self, id: u64) -> Option<&mut Invocation> {
        self.invocations.get_mut(&id)
    }
}
