//! Function invocation.
use std::ops::{Index, IndexMut, Range};

/// Invocation status.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum InvocationStatus {
    /// Invocation is registered, but simulation time has not reached its arrival time yet.
    NotArrived,
    /// Invocation is queued at one of the invokers.
    Queued,
    /// Invocation is waiting for the assigned container to start.
    WaitingForContainer,
    /// Invocation is running.
    Running,
    /// Invocation is finished.
    Finished,
}

/// Function invocation.
#[derive(Copy, Clone)]
pub struct Invocation {
    /// Invocation id.
    pub id: usize,
    /// [`crate::function::Application`] id.
    pub app_id: usize,
    /// [`crate::function::Function`] id.
    pub func_id: usize,
    /// Invocation duration.
    pub duration: f64,
    /// Invocation request arrival time.
    pub arrival_time: f64,
    /// Invocation status.
    pub status: InvocationStatus,
    /// [`crate::host::Host`] id if the invocation was scheduled on some host.
    pub host_id: Option<usize>,
    /// [`crate::container::Container`] id if the invocation was assigned to some container.
    pub container_id: Option<usize>,
    /// Execution start time.
    pub start_time: Option<f64>,
    /// Execution finish time.
    pub finish_time: Option<f64>,
}

impl Invocation {
    /// Returns invocation execution time (finish - start).
    pub fn execution_time(&self) -> f64 {
        self.finish_time.unwrap() - self.start_time.unwrap()
    }

    /// Returns invocation response time (finish - arrival).
    pub fn response_time(&self) -> f64 {
        self.finish_time.unwrap() - self.arrival_time
    }

    /// Returns invocation wait time (start - arrival).
    pub fn wait_time(&self) -> f64 {
        self.start_time.unwrap() - self.arrival_time
    }
}

/// Stores information about function invocations.
#[derive(Default)]
pub struct InvocationRegistry {
    invocations: Vec<Invocation>,
}

impl InvocationRegistry {
    /// Adds a new invocation to the registry and returns its `id`.
    pub fn add_invocation(&mut self, app_id: usize, func_id: usize, duration: f64, arrival_time: f64) -> usize {
        let id = self.invocations.len();
        let invocation = Invocation {
            id,
            app_id,
            func_id,
            duration,
            arrival_time,
            status: InvocationStatus::NotArrived,
            host_id: None,
            container_id: None,
            start_time: None,
            finish_time: None,
        };
        self.invocations.push(invocation);
        id
    }

    /// Returns the number of invocations in the registry.
    pub fn len(&self) -> usize {
        self.invocations.len()
    }

    /// Returns whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.invocations.is_empty()
    }
}

impl Index<usize> for InvocationRegistry {
    type Output = Invocation;

    fn index(&self, index: usize) -> &Self::Output {
        &self.invocations[index]
    }
}

impl Index<Range<usize>> for InvocationRegistry {
    type Output = [Invocation];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.invocations[index]
    }
}

impl IndexMut<usize> for InvocationRegistry {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.invocations[index]
    }
}
