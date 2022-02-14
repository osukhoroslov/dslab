use crate::event::Event;

pub trait EventHandler {
    fn on(&mut self, event: Event);
}

#[macro_export]
macro_rules! cast {
    ( match $event:ident.data { $( $pattern:pat => $arm:block )+ } ) => {
        $(
            if let Some($pattern) = $event.data.downcast_ref() {
                $arm
            } else
        )*
        {
            println!("Unhandled event: {:?}", $event)
        }
    }
}
