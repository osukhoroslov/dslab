//! Definition of the throughput sharing model trait.

use sugars::boxed;

use dslab_core::context::SimulationContext;

/// Trait for throughput sharing model.
pub trait ThroughputSharingModel<T> {
    /// Adds new activity into the model.
    ///
    /// Activity is represented by `item`, has amount of work `value` and starts at `ctx.time()`.
    fn insert(&mut self, item: T, volume: f64, ctx: &mut SimulationContext);
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

/// Helper for creating throughput function which always returns given value.
pub fn make_constant_throughput_function(throughput: f64) -> ThroughputFunction {
    boxed!(move |_| throughput)
}
