//! Core elements of the throughput sharing model.

use dslab_core::context::SimulationContext;

/// Type alias for activity identifier.
pub type ActivityId = u64;

/// Trait for throughput sharing model.
pub trait ThroughputSharingModel<T> {
    /// Adds new activity into the model.
    ///
    /// Activity is represented by `item`, has amount of work `volume` and starts at `ctx.time()`.
    fn insert(&mut self, item: T, volume: f64, ctx: &SimulationContext) -> ActivityId;
    /// Returns the next activity completion time (if any) along with corresponding activity item.
    ///
    /// The returned activity is removed from the model.
    fn pop(&mut self) -> Option<(f64, T)>;
    /// Cancels the activity with given `id` at `ctx.time()`.
    ///
    /// If the activity is still running returns the activity and the completed work volume, otherwise returns `None`.
    fn cancel(&mut self, _id: ActivityId, _ctx: &SimulationContext) -> Option<(f64, T)> {
        unimplemented!()
    }
    /// Returns the next activity completion time (if any) along with corresponding activity item.
    ///
    /// In contrast to `pop`, the returned activity is not removed from the model.
    fn peek(&mut self) -> Option<(f64, &T)>;
}

/// Function that computes the total resource throughput based on the number of concurrent activities.
///
/// It can be used to model the performance degradation caused by interference, resource contention, etc.
pub type ResourceThroughputFn = Box<dyn Fn(usize) -> f64>;

/// Provides the function that computes the throughput factor per activity.
///
/// It can be used to model the dependence of throughput achieved by the activity on its properties
/// (e.g. amount of work or data) or to model the variability of throughput based on some distribution.
///
/// Note that the dependence of activity throughput on other concurrent activities
/// (resource contention, interference, etc.) is supposed to be modeled by the `ResourceThroughputFn`.
pub trait ActivityFactorFn<T> {
    /// Returns the throughput factor for activity represented by `item`.
    ///
    /// The factor per each activity is computed only once when the activity is arrived.
    /// The simulation context is provided to enable access to the current simulation time and the random engine.
    ///
    /// The factor value is used to obtain the effective activity throughput as follows:
    /// `effective_throughput = allocated_throughput * factor`.
    /// The factor value below 1 means that the activity does not fully utilize the allocated throughput.
    /// The factor value above 1 means that the activity somehow surpasses the allocated throughput.
    ///
    /// To achieve the desired behaviour the implementations of `ThroughputSharingModel` actually use the factor value
    /// to scale the activity volume. When a new activity is added to a model, the factor value obtained from
    /// `get_factor` is used to scale the activity volume as follows: `new_volume = volume / factor`. This achieves
    /// the same effect as changing the effective activity throughput.
    fn get_factor(&mut self, item: &T, ctx: &SimulationContext) -> f64;
}
