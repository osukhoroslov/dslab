//! Definition of the throughput model trait.

/// Trait for throughput sharing model.
pub trait ThroughputSharingModel<T> {
    /// Adds new activity into the model.
    ///
    /// Activity starts at `current_time`, has amount of work `value` and is represented by `item`.
    fn insert(&mut self, current_time: f64, volume: f64, item: T);
    /// Returns the next activity completion time (if any) along with corresponding activity item.
    fn pop(&mut self) -> Option<(f64, T)>;
    /// Returns reference to the next request of the model.
    fn peek(&self) -> Option<(f64, &T)>;
}

/// Type alias for function used to describe the dependence of resource throughput on the number of concurrent activities.
pub type ThroughputFunction = Box<dyn Fn(usize) -> f64>;
