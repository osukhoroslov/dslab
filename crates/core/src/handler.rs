use log::error;

use crate::event::Event;

pub trait EventHandler {
    fn on(&mut self, event: Event);
}

pub fn _log_unhandled_event(event: Event) {
    error!(
        "[{:.3} {} simulation] Unhandled event: {}",
        event.time.into_inner(),
        crate::log::get_colored("ERROR", colored::Color::Red),
        serde_json::to_string(&event).unwrap()
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
