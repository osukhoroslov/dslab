//! Definition of the throughput sharing model trait.

/// Trait for throughput sharing model.
pub trait Model<T> {
    /// Adds new activity into the model.
    ///
    /// Activity starts at `current_time`, has amount of work `value` and is represented by `item`.
    fn insert(&mut self, current_time: f64, volume: f64, item: T);
    /// Returns the next activity completion time (if any) along with corresponding activity item.
    ///
    /// The returned activity is removed from the model.
    fn pop(&mut self) -> Option<(f64, T)>;
    /// Returns the next activity completion time (if any) along with corresponding activity item.
    ///
    /// In contrast to `pop`, the returned activity is not removed from the model.
    fn peek(&self) -> Option<(f64, &T)>;
}

/// Type alias for function used to describe the dependence of resource throughput on the number of concurrent
/// activities.
pub type ThroughputFunction = Box<dyn Fn(usize) -> f64>;
