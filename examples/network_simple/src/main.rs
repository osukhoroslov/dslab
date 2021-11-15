use std::cell::RefCell;
use std::collections::{HashMap};
use std::rc::Rc;

use core::sim::Simulation;
use core::actor::{Actor, ActorId, ActorContext};
use crate::Event::*;

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum Event {
    SendMessage {
        message: Message
    },
    ReceiveMessage_ {
        message: Message
    },
    SendData {
        data: Data
    },
    ReceiveData_ {
        data: Data
    },
}

// NETWORK TYPES ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Data {
    id: usize,
    source: ActorId,
    dest: ActorId,
    size: f64,    
}

#[derive(Debug, Clone)]
pub struct Message {
    id: usize,
    source: ActorId,
    dest: ActorId,
    data: String,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext<Event>);
    fn recieve_data(&mut self, data: Data, ctx: &mut ActorContext<Event>);
}

pub trait NetworkModel: DataOperation { }

// NETWORK MODELs ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct ConstantThroughputNetwork {
    throughput: f64,
}

impl ConstantThroughputNetwork {
    pub fn new(throughput: f64) -> ConstantThroughputNetwork {
        return ConstantThroughputNetwork{throughput};
    }
}

impl DataOperation for ConstantThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext<Event>) {
        let new_message_delivery_time = data.size / self.throughput;
        println!("Data ID: {}, From: {}, To {}, Size: {}, Time: {}", data.id, data.source, data.dest, data.size, new_message_delivery_time);
        ctx.emit(ReceiveData_{data}, ctx.id.clone(), new_message_delivery_time);
    }

    fn recieve_data(&mut self, data: Data, _ctx: &mut ActorContext<Event>) {
        println!("Data ID: {}, From: {}, To {}, Size: {}", data.id, data.source, data.dest, data.size);
    }
}

impl NetworkModel for ConstantThroughputNetwork {}


#[derive(Debug, Clone)]
struct SendDataProgress {
    size_left: f64,
    last_speed: f64,
    last_time: f64,
    recieve_event: u64,
    data: Data,
}

#[derive(Debug, Clone)]
struct SharedThroughputNetwork {
    throughput: f64,
    cur: HashMap<usize, SendDataProgress>,
}

impl SharedThroughputNetwork {
    pub fn new(throughput: f64) -> SharedThroughputNetwork {
        return SharedThroughputNetwork{throughput, cur: HashMap::new()};
    }

    fn recalculate_recieve_time(&mut self, ctx: &mut ActorContext<Event>) {
        let cur_time = ctx.time();
        for (_, send_elem) in self.cur.iter_mut() {
            send_elem.size_left -= (cur_time - send_elem.last_time) * send_elem.last_speed;
            ctx.cancel_event(send_elem.recieve_event);
        }

        let new_throughput = self.throughput / (self.cur.len() as f64);

        for (_, send_elem) in self.cur.iter_mut() {
            send_elem.last_speed = new_throughput;
            send_elem.last_time = cur_time;
            let data_delivery_time = send_elem.size_left / new_throughput;
            send_elem.recieve_event = ctx.emit(ReceiveData_ { data: send_elem.data.clone()}, ctx.id.clone(), data_delivery_time);
            println!("Calculate Recieve Time. Data ID: {}, From: {}, To {}, Size: {}, SizeLeft: {}, New Time: {}", send_elem.data.id, send_elem.data.source,
                send_elem.data.dest, send_elem.data.size, send_elem.size_left, data_delivery_time);
        }
    }
}

impl DataOperation for SharedThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext<Event>) {

        let new_send_data_progres = SendDataProgress{
            size_left: data.size,
            last_speed: 0.,
            last_time: 0.,
            recieve_event: 0,
            data: data,
        };

        let data_id = new_send_data_progres.data.id;
        if self.cur.insert(data_id, new_send_data_progres).is_some() {
            panic!("SharedThroughputNetwork: data with id {} already exist", data_id);
        }

        self.recalculate_recieve_time(ctx);
    }

    fn recieve_data(&mut self, data: Data, ctx: &mut ActorContext<Event>) {
        println!("Recieved. Data ID: {}, From: {}, To {}, Size: {}", data.id, data.source, data.dest, data.size);
        self.cur.remove(&data.id);
        self.recalculate_recieve_time(ctx);
    }
}

impl NetworkModel for SharedThroughputNetwork {}


// ACTORS //////////////////////////////////////////////////////////////////////////////////////////

pub struct NetworkActor {
    network_model : Rc<RefCell<dyn NetworkModel>>,
    min_delay: f64
}

impl NetworkActor {
    pub fn new(network_model : Rc<RefCell<dyn NetworkModel>>) -> Self {
        Self {network_model, min_delay: 0.1}
    }
}

impl Actor<Event> for NetworkActor {
    fn on(&mut self, event: Event, _from: ActorId, ctx: &mut ActorContext<Event>) {
        match event {
            Event::SendMessage { message } => {
                println!("{} send Message '{}' to {}", message.source, message.data, message.dest);
                ctx.emit(ReceiveMessage_ { message }, ctx.id.clone(), self.min_delay);
            }
            Event::ReceiveMessage_ { message } => {
                println!("{} received Message '{}' from {}", message.dest, message.data, message.source);
                // here should be message delivery from network
            }
            Event::SendData { data } => {
                self.network_model.borrow_mut().send_data(data, ctx)
            }
            Event::ReceiveData_ { data } => {
                self.network_model.borrow_mut().recieve_data( data, ctx )
                // here should be data delivery by network
            }
        }
    }
    
    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    let mut sim = Simulation::<Event>::new(123);
    let sender_actor = ActorId::from("sender");
    let reciever_actor = ActorId::from("reciever");

    let shared_network_model = Rc::new(RefCell::new(SharedThroughputNetwork::new(10.0)));
    let shared_network = Rc::new(RefCell::new(NetworkActor::new(shared_network_model)));
    sim.add_actor("shared_network", shared_network);

    let msg = Message { id: 0, source: sender_actor.clone(), dest: reciever_actor.clone(), data: "Hello World".to_string()};

    let data1 = Data{ id: 1, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 100.0};
    sim.add_event(SendData { data: data1 }, &sender_actor.0, "shared_network", 0.);

    let data2 = Data{ id: 2, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 1000.0};
    sim.add_event(SendData { data: data2 }, &sender_actor.0, "shared_network", 0.);

    let data3 = Data{ id: 3, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 5.0};
    sim.add_event(SendData { data: data3 }, &sender_actor.0, "shared_network", 0.);

    sim.add_event(SendMessage {message: msg}, &sender_actor.0, "shared_network", 0.);

    sim.step_until_no_events();

    
    let constant_network_model = Rc::new(RefCell::new(ConstantThroughputNetwork::new(10.0)));
    let constant_network = Rc::new(RefCell::new(NetworkActor::new(constant_network_model)));
    sim.add_actor("constant_network", constant_network);
    let msg = Message { id: 0, source: sender_actor.clone(), dest: reciever_actor.clone(), data: "Hello World".to_string()};

    let data1 = Data{ id: 1, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 100.0};
    sim.add_event(SendData { data: data1 }, &sender_actor.0, "constant_network", 0.);

    let data2 = Data{ id: 2, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 1000.0};
    sim.add_event(SendData { data: data2 }, &sender_actor.0, "constant_network", 0.);

    let data3 = Data{ id: 3, source: sender_actor.clone(), dest: reciever_actor.clone(), size: 5.0};
    sim.add_event(SendData { data: data3 }, &sender_actor.0, "constant_network", 0.);

    sim.add_event(SendMessage {message: msg}, &sender_actor.0, "constant_network", 0.);

    sim.step_until_no_events();
}