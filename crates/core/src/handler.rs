use log::error;
use serde_json::json;
use serde_type_name::type_name;

use crate::event::Event;

pub trait EventHandler {
    fn on(&mut self, event: Event);
}

pub fn _log_unhandled_event(event: Event) {
    error!(
        target: "simulation",
        "[{:.3} {} simulation] Unhandled event: {}",
        event.time.into_inner(),
        crate::log::get_colored("ERROR", colored::Color::Red),
        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": event.src, "dest": event.dest})
    );
}

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
            $crate::handler::_log_unhandled_event($event);
        }
    }
}
