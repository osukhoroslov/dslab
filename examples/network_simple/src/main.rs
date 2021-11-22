use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;
use std::sync::atomic::{AtomicUsize, Ordering};

// Counter for network ids
static COUNTER: AtomicUsize = AtomicUsize::new(0);

// Logging ///////////////////////////////////////////////////////////////////////////////////
#[derive(Debug, Clone)]
pub enum LogLevel {
    Empty = 0,
    SendRecieve = 1,
    Full = 2,
}

fn check_log_level(log_level: LogLevel, expected_log_level: LogLevel) -> bool {
    return (log_level as usize) & (expected_log_level as usize) != 0;
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

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SendMessage {
    message: Message,
}

#[derive(Debug)]
pub struct ReceiveMessage_ {
    message: Message,
}

#[derive(Debug)]
pub struct ReceiveMessage {
    message: Message,
}

#[derive(Debug)]
pub struct SendData {
    data: Data,
}

#[derive(Debug)]
pub struct ReceiveData_ {
    data: Data,
}

#[derive(Debug)]
pub struct ReceiveData {
    data: Data,
}

// NETWORK MODEL TEMPLATE ///////////////////////////////////////////////////////////////////////////////////

pub trait DataOperation {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext);
    fn recieve_data(&mut self, data: Data, ctx: &mut ActorContext);
    fn set_network_params(&mut self, min_delay: f64);
}

pub trait LogProperties {
    fn set_log_level(&mut self, log_level: LogLevel);
}

pub trait NetworkModel: DataOperation + LogProperties {}

// NETWORK MODELs ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct ConstantThroughputNetwork {
    throughput: f64,
    min_delay: f64,
    log_level: LogLevel,
}

impl ConstantThroughputNetwork {
    pub fn new(throughput: f64) -> ConstantThroughputNetwork {
        return ConstantThroughputNetwork {
            throughput,
            min_delay: 0.,
            log_level: LogLevel::Empty,
        };
    }
}

impl DataOperation for ConstantThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        let new_message_delivery_time = data.size / self.throughput + self.min_delay;
        println!(
            "Data ID: {}, From: {}, To {}, Size: {}, Time: {}",
            data.id, data.source, data.dest, data.size, new_message_delivery_time
        );
        ctx.emit(
            ReceiveData_ { data },
            &ctx.id.clone(),
            new_message_delivery_time,
        );
    }

    fn recieve_data(&mut self, data: Data, _ctx: &mut ActorContext) {
        println!(
            "Data ID: {}, From: {}, To {}, Size: {}",
            data.id, data.source, data.dest, data.size
        );
    }

    fn set_network_params(&mut self, min_delay: f64) {
        self.min_delay = min_delay;
    }
}

impl LogProperties for ConstantThroughputNetwork {
    fn set_log_level(&mut self, log_level: LogLevel) {
        self.log_level = log_level;
    }
}

impl NetworkModel for ConstantThroughputNetwork {}

#[derive(Debug, Clone)]
struct SendDataProgress {
    delay_left: f64,
    size_left: f64,
    last_speed: f64,
    last_time: f64,
    recieve_event: u64,
    data: Data,
}

#[derive(Debug, Clone)]
struct SharedThroughputNetwork {
    throughput: f64,
    cur: BTreeMap<usize, SendDataProgress>,
    min_delay: f64,
    log_level: LogLevel,
}

impl SharedThroughputNetwork {
    pub fn new(throughput: f64) -> SharedThroughputNetwork {
        return SharedThroughputNetwork {
            throughput,
            cur: BTreeMap::new(),
            min_delay: 0.,
            log_level: LogLevel::Empty,
        };
    }

    fn recalculate_recieve_time(&mut self, ctx: &mut ActorContext) {
        let cur_time = ctx.time();
        for (_, send_elem) in self.cur.iter_mut() {
            let mut delivery_time = cur_time - send_elem.last_time;
            if delivery_time > send_elem.delay_left {
                delivery_time -= send_elem.delay_left;
                send_elem.delay_left = 0.0;
            } else {
                send_elem.delay_left -= delivery_time;
                delivery_time = 0.0;
            }
            send_elem.size_left -= delivery_time * send_elem.last_speed;
            ctx.cancel_event(send_elem.recieve_event);
        }

        let new_throughput = self.throughput / (self.cur.len() as f64);

        for (_, send_elem) in self.cur.iter_mut() {
            send_elem.last_speed = new_throughput;
            send_elem.last_time = cur_time;
            let data_delivery_time = send_elem.size_left / new_throughput + send_elem.delay_left;
            send_elem.recieve_event = ctx.emit(
                ReceiveData_ {
                    data: send_elem.data.clone(),
                },
                &ctx.id.clone(),
                data_delivery_time,
            );
            if check_log_level(self.log_level.clone(), LogLevel::Full) {
                println!("Calculate Recieve Time. Data ID: {}, From: {}, To {}, Size: {}, SizeLeft: {}, New Time: {}", send_elem.data.id, send_elem.data.source,
                    send_elem.data.dest, send_elem.data.size, send_elem.size_left, data_delivery_time);
            }
        }
    }
}

impl DataOperation for SharedThroughputNetwork {
    fn send_data(&mut self, data: Data, ctx: &mut ActorContext) {
        if check_log_level(self.log_level.clone(), LogLevel::SendRecieve) {
            println!(
                "System time: {}, Send. Data ID: {}, From: {}, To {}, Size: {}",
                ctx.time(),
                data.id,
                data.source,
                data.dest,
                data.size.clone()
            );
        }

        let new_send_data_progres = SendDataProgress {
            delay_left: self.min_delay,
            size_left: data.size,
            last_speed: 0.,
            last_time: 0.,
            recieve_event: 0,
            data: data,
        };

        let data_id = new_send_data_progres.data.id;
        if self.cur.insert(data_id, new_send_data_progres).is_some() {
            panic!(
                "SharedThroughputNetwork: data with id {} already exist",
                data_id
            );
        }

        self.recalculate_recieve_time(ctx);
    }

    fn recieve_data(&mut self, data: Data, ctx: &mut ActorContext) {
        if check_log_level(self.log_level.clone(), LogLevel::SendRecieve) {
            println!(
                "System time: {}, Recieved. Data ID: {}, From: {}, To {}, Size: {}",
                ctx.time(),
                data.id,
                data.source,
                data.dest,
                data.size
            );
        }
        self.cur.remove(&data.id);
        self.recalculate_recieve_time(ctx);
    }

    fn set_network_params(&mut self, min_delay: f64) {
        self.min_delay = min_delay;
    }
}

impl LogProperties for SharedThroughputNetwork {
    fn set_log_level(&mut self, log_level: LogLevel) {
        self.log_level = log_level;
    }
}

impl NetworkModel for SharedThroughputNetwork {}

// NETWORK ACTOR //////////////////////////////////////////////////////////////////////////////////////////

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
    fn on(&mut self, event: Box<dyn Event>, _from: &ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            SendMessage { message } => {
                if check_log_level(self.log_level.clone(), LogLevel::SendRecieve){
                    println!("System time: {}, {} send Message '{}' to {}", ctx.time(), message.source, message.data, message.dest);
                }
                ctx.emit(ReceiveMessage_ { message: message.clone() }, &ctx.id.clone(), self.min_delay);
            },
            ReceiveMessage_ { message } => {
                if check_log_level(self.log_level.clone(), LogLevel::SendRecieve){
                    println!("System time: {}, {} received Message '{}' from {}", ctx.time(), message.dest, message.data, message.source);
                }
                ctx.emit(ReceiveMessage {message: message.clone()}, &message.dest, 0.0);
            },
            SendData { data } => {
                self.network_model.borrow_mut().send_data(data.clone(), ctx);
            },
            ReceiveData_ { data } => {
                self.network_model.borrow_mut().recieve_data( data.clone(), ctx );
                ctx.emit(ReceiveData {data: data.clone()}, &data.dest, 0.0);
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// ACTORS //////////////////////////////////////////////////////////////////////////////////////////
#[derive(Debug)]
pub struct Start {
    size: f64,
    receiver_id: ActorId,
}

pub struct DataSender {
    network_id: ActorId,
}

impl DataSender {
    pub fn new(network_id: ActorId) -> Self {
        Self { network_id }
    }
}

impl Actor for DataSender {
    fn on(&mut self, event: Box<dyn Event>, _from: &ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start { size, receiver_id } => {
                let data_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                let data = Data{ id: data_id, source: ctx.id.clone(), dest: receiver_id.clone(), size: *size};
                ctx.emit(SendData { data }, &self.network_id, 0.0);
            },
            ReceiveData { data: _ } => {
                println!("Sender: {} Done", ctx.id.clone());
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

pub struct DataReceiver {
    network_id: ActorId,
}

impl DataReceiver {
    pub fn new(network_id: ActorId) -> Self {
        Self { network_id }
    }
}

impl Actor for DataReceiver {
    fn on(&mut self, event: Box<dyn Event>, _from: &ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            ReceiveData { data } => {
                let data_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                let new_size = 1000.0 - data.size;
                let data = Data{ id: data_id, source: ctx.id.clone(), dest: data.source.clone(), size: new_size};
                ctx.emit(SendData { data }, &self.network_id, 0.0);
                println!("Reciever: {} Done", ctx.id.clone());
            },
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn main() {
    let process_simple_data_send = false;
    let process_simple_message_send = false;
    let process_check_order = false;
    let process_with_actors = true;

    let mut sim = Simulation::new(123);
    let sender_actor = ActorId::from("sender");
    let reciever_actor = ActorId::from("reciever");

    let shared_network_model = Rc::new(RefCell::new(SharedThroughputNetwork::new(10.0)));
    let shared_network = Rc::new(RefCell::new(NetworkActor::new_with_log(
        shared_network_model,
        LogLevel::SendRecieve,
    )));
    let shared_network_actor = sim.add_actor("shared_network", shared_network);

    let constant_network_model = Rc::new(RefCell::new(ConstantThroughputNetwork::new(10.0)));
    let constant_network = Rc::new(RefCell::new(NetworkActor::new_with_log(
        constant_network_model,
        LogLevel::SendRecieve,
    )));
    let constant_network_actor = sim.add_actor("constant_network", constant_network);

    if process_simple_data_send {
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            data: "Hello World".to_string(),
        };

        let data1 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 100.0,
        };
        sim.add_event(
            SendData { data: data1 },
            &sender_actor,
            &shared_network_actor,
            0.,
        );

        let data2 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 1000.0,
        };
        sim.add_event(
            SendData { data: data2 },
            &sender_actor,
            &shared_network_actor,
            0.,
        );

        let data3 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 5.0,
        };
        sim.add_event(
            SendData { data: data3 },
            &sender_actor,
            &shared_network_actor,
            0.,
        );

        sim.add_event(
            SendMessage { message: msg },
            &sender_actor,
            &shared_network_actor,
            0.,
        );

        sim.step_until_no_events();
    }

    if process_simple_message_send {
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            data: "Hello World".to_string(),
        };

        let data1 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 100.0,
        };
        sim.add_event(
            SendData { data: data1 },
            &sender_actor,
            &constant_network_actor,
            0.,
        );

        let data2 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 1000.0,
        };
        sim.add_event(
            SendData { data: data2 },
            &sender_actor,
            &constant_network_actor,
            0.,
        );

        let data3 = Data {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            size: 5.0,
        };
        sim.add_event(
            SendData { data: data3 },
            &sender_actor,
            &constant_network_actor,
            0.,
        );

        sim.add_event(
            SendMessage { message: msg },
            &sender_actor,
            &constant_network_actor,
            0.,
        );

        sim.step_until_no_events();
    }

    if process_check_order {
        let msg = Message {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
            source: sender_actor.clone(),
            dest: reciever_actor.clone(),
            data: "Hello World".to_string(),
        };

        for _i in 1..10 {
            let data1 = Data {
                id: COUNTER.fetch_add(1, Ordering::Relaxed),
                source: sender_actor.clone(),
                dest: reciever_actor.clone(),
                size: 1000.0,
            };
            sim.add_event(
                SendData { data: data1 },
                &sender_actor,
                &shared_network_actor,
                0.,
            );
        }

        sim.add_event(
            SendMessage { message: msg },
            &sender_actor,
            &shared_network_actor,
            0.,
        );

        sim.step_until_no_events();
    }

    if process_with_actors {
        let mut receivers = Vec::new();
        let mut senders = Vec::new();

        for i in 1..10 {
            let receiver_id = "receiver_".to_string() + &i.to_string();
            let receiver = Rc::new(RefCell::new(DataReceiver::new(
                shared_network_actor.clone(),
            )));
            let receiver_actor = sim.add_actor(&receiver_id, receiver);
            receivers.push(receiver_actor);

            let sender_id = "sender_".to_string() + &i.to_string();
            let sender = Rc::new(RefCell::new(DataSender::new(shared_network_actor.clone())));
            let sender_actor = sim.add_actor(&sender_id, sender);
            senders.push(sender_actor);
        }

        let initial_actor = ActorId::from("app");
        for i in 1..10 {
            sim.add_event(
                Start {
                    size: (i as f64) * 100.0,
                    receiver_id: receivers[i - 1].clone(),
                },
                &initial_actor,
                &senders[i - 1],
                0.0,
            );
        }

        sim.step_until_no_events();
    }
}
