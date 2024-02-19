//! Event handling.

use crate::event::Event;

/// Trait for consuming events in simulation components.
pub trait EventHandler {
    /// Processes event.
    ///
    /// You can implement any processing logic here.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     state: u32,
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(some_field, 16);
    ///                 self.state = some_field;
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let comp2 = Rc::new(RefCell::new(Component { state: 0, ctx: comp2_ctx }));
    /// let comp2_id = sim.add_handler("comp2", comp2.clone());
    /// comp1_ctx.emit(SomeEvent{ some_field: 16 }, comp2_id, 1.2);
    /// assert_eq!(comp2.borrow().state, 0);
    /// sim.step();
    /// assert_eq!(comp2.borrow().state, 16);
    /// ```
    fn on(&mut self, event: Event);
}

/// Enables the use of pattern matching syntax for processing different types of events
/// by downcasting the event payload from [`EventData`](crate::event::EventData) to user-defined types.
///
/// Note that match arms need not be exhaustive. However, if the event payload does not match any of specified arms,
/// the macro will log the event as unhandled under `ERROR` level.  
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
/// use serde::Serialize;
/// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
///
/// #[derive(Clone, Serialize)]
/// pub struct SomeEvent {
///     some_field: u32,
/// }
///
/// #[derive(Clone, Serialize)]
/// pub struct AnotherEvent {
///     another_field: f64,
/// }
///
/// pub struct Component {
///     ctx: SimulationContext,
/// }
///
/// impl EventHandler for Component {
///     fn on(&mut self, event: Event) {
///         cast!(match event.data {
///             SomeEvent { some_field } => {
///                 // some event processing logic...
///             }
///             AnotherEvent { another_field } => {
///                 // some event processing logic...
///             }
///         })
///
///    }
/// }
///
/// let mut sim = Simulation::new(123);
/// let mut comp_ctx = sim.create_context("comp");
/// let comp_id = sim.add_handler("comp", Rc::new(RefCell::new(Component { ctx: comp_ctx })));
/// let client_ctx = sim.create_context("client");
/// client_ctx.emit(SomeEvent{ some_field: 16 }, comp_id, 1.2);
/// client_ctx.emit(AnotherEvent{ another_field: 1.6 }, comp_id, 2.5);
/// sim.step_until_no_events();
/// ```
#[macro_export]
macro_rules! cast {
    ( match $event:ident.data { $( $type:ident { $($tt:tt)* } => { $($expr:tt)* } )+ } ) => {
        $(
            if $event.data.is::<$type>() {
                if let Ok(__value) = $event.data.downcast::<$type>() {
                    let $type { $($tt)* } = *__value;
                    $($expr)*
                }
            } else
        )*
        {
            $crate::log::log_unhandled_event($event);
        }
    }
}

/// Specifies which pending events are cancelled on event handler removal.
pub enum EventCancellationPolicy {
    /// Cancel events destined to the component.
    Incoming,
    /// Cancel events produced by the component.
    Outgoing,
    /// Cancel all events related to the component.
    All,
    /// Do not cancel events.
    None,
}
