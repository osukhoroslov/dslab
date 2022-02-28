use crate::event::Event;

pub trait EventHandler {
    fn on(&mut self, event: Event);
}

#[macro_export]
macro_rules! cast {
    ( match $event:ident.data { $( $type:ident { $($tt:tt)* } => { $($expr:tt)* } )+ } ) => {
        $(
            if $event.data.is::<$type>() {
                let $type { $($tt)* } = *$event.data.downcast::<$type>().unwrap();
                $($expr)*
            } else
        )*
        {
            println!("Unhandled event: {:?}", $event)
        }
    }
}
