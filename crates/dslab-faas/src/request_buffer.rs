use std::collections::VecDeque;

use crate::invocation::InvocationRequest;

#[derive(Default)]
pub struct RequestBuffer {
    data: VecDeque<InvocationRequest>,
    update_id: u64,
}

impl RequestBuffer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn try_update(&mut self, id: u64) -> Option<InvocationRequest> {
        if id == self.update_id {
            if let Some(request) = self.data.pop_front() {
                self.update_id = request.id;
                return Some(request);
            }
        }
        None
    }

    pub fn set_update_id(&mut self, id: u64) {
        self.update_id = id;
    }

    pub fn push(&mut self, request: InvocationRequest) {
        self.data.push_back(request);
    }
}
