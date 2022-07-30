//! Definition of the throughput model trait.

/// Trait for throughput sharing model.
pub trait ThroughputSharingModel<T> {
    /// Inserts new `item` with given `volume` in the model.
    fn insert(&mut self, current_time: f64, volume: f64, item: T);
    /// Pops next request from the model if it is not empty.
    fn pop(&mut self) -> Option<(f64, T)>;
    /// Returns reference to the next request of the model.
    fn peek(&self) -> Option<(f64, &T)>;
}

/// Type alias for function of dynamic throughput
pub type ThroughputFunction = Box<dyn Fn(usize) -> f64>;
