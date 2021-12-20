use std::cell::RefCell;
use std::rc::Rc;
use log::info;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

use crate::model::*;

pub struct NetworkActor {
    network_model: Rc<RefCell<dyn NetworkModel>>
}

impl NetworkActor {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>) -> Self {
        Self {
            network_model
        }
    }

}

impl Actor for NetworkActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            MessageSend { message } => {
                info!("System time: {}, {} send Message '{}' to {}", ctx.time(), message.source, message.data, message.dest);
                ctx.emit(MessageReceive { message: message.clone() }, ctx.id.clone(), self.network_model.borrow().delay());
            },
            MessageReceive { message } => {
                info!("System time: {}, {} received Message '{}' from {}", ctx.time(), message.dest, message.data, message.source);
                ctx.emit(MessageDelivery {message: message.clone()}, message.dest.clone(), 0.0);
            },
            DataTransferRequest { data } => {
                self.network_model.borrow_mut().send_data(data.clone(), ctx);
            },
            DataReceive { data } => {
                self.network_model.borrow_mut().receive_data( data.clone(), ctx );
                ctx.emit(DataDelivery {data: data.clone()}, data.dest.clone(), 0.0);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}