//! Event handling.

use crate::event::Event;

/// Trait for consuming events in simulation components.
pub trait EventHandler {
    /// Processes event.
    fn on(&mut self, event: Event);
}

/// Enables the use of pattern matching syntax for processing different types of events
/// by downcasting the event payload from [`crate::event::EventData`] to user-defined types.
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
