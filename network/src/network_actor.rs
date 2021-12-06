use std::cell::RefCell;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;

use crate::model::*;

pub struct NetworkActor {
    network_model: Rc<RefCell<dyn NetworkModel>>,
    min_delay: f64,
    log_level: LogLevel,
}

impl NetworkActor {
    pub fn new(network_model: Rc<RefCell<dyn NetworkModel>>) -> Self {
        network_model.borrow_mut().set_network_params(0.1);
        Self {
            network_model,
            min_delay: 0.1,
            log_level: LogLevel::Empty,
        }
    }

    pub fn new_with_log(network_model: Rc<RefCell<dyn NetworkModel>>, log_level: LogLevel) -> Self {
        network_model.borrow_mut().set_log_level(log_level.clone());
        network_model.borrow_mut().set_network_params(0.1);
        Self {
            network_model,
            min_delay: 0.1,
            log_level,
        }
    }
}

impl Actor for NetworkActor {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            SendMessage { message } => {
                if check_log_level(self.log_level.clone(), LogLevel::SendReceive){
                    println!("System time: {}, {} send Message '{}' to {}", ctx.time(), message.source, message.data, message.dest);
                }
                ctx.emit(ReceiveMessage_ { message: message.clone() }, ctx.id.clone(), self.min_delay);
            },
            ReceiveMessage_ { message } => {
                if check_log_level(self.log_level.clone(), LogLevel::SendReceive){
                    println!("System time: {}, {} received Message '{}' from {}", ctx.time(), message.dest, message.data, message.source);
                }
                ctx.emit(ReceiveMessage {message: message.clone()}, message.dest.clone(), 0.0);
            },
            SendData { data } => {
                self.network_model.borrow_mut().send_data(data.clone(), ctx);
            },
            ReceiveData_ { data } => {
                self.network_model.borrow_mut().receive_data( data.clone(), ctx );
                ctx.emit(ReceiveData {data: data.clone()}, data.dest.clone(), 0.0);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}