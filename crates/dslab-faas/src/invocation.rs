use serde::Serialize;

use crate::util::{Counter, VecMap};

#[derive(Copy, Clone, Debug, Serialize)]
pub struct InvocationRequest {
    pub func_id: usize,
    pub duration: f64,
    pub time: f64,
    pub id: usize,
}

#[derive(Copy, Clone)]
pub struct Invocation {
    pub id: usize,
    pub request: InvocationRequest,
    pub host_id: usize,
    pub container_id: usize,
    pub started: f64,
    pub finished: Option<f64>,
}

#[derive(Default)]
pub struct InvocationRegistry {
    invocation_ctr: Counter,
    invocations: VecMap<Invocation>,
}

impl InvocationRegistry {
    pub fn new_invocation(&mut self, request: InvocationRequest, host_id: usize, container_id: usize, time: f64) {
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

    pub fn register_invocation(&mut self) -> usize {
        self.invocation_ctr.increment()
    }

    pub fn get_invocation(&self, id: usize) -> Option<&Invocation> {
        self.invocations.get(id)
    }

    pub fn get_invocation_mut(&mut self, id: usize) -> Option<&mut Invocation> {
        self.invocations.get_mut(id)
    }
}
