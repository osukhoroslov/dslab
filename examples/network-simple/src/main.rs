use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use clap::Parser;
use env_logger::Builder;
use sugars::{boxed, rc, refcell};

use dslab_core::{cast, log_info, Event, EventHandler, Id, Simulation, SimulationContext};

use dslab_network::models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel};
use dslab_network::{DataTransferCompleted, MessageDelivered, Network};

/// Demonstrates the use of simple network models from dslab-network.
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// Network model to use.
    #[arg(long = "model")]
    model: NetworkModel,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum NetworkModel {
    Constant,
    Shared,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
    let args = Args::parse();

    let mut sim = Simulation::new(123);

    // create network with specified model, add nodes and register in simulation
    println!("Used network model: {:?}BandwidthNetworkModel", args.model);
    let mut net = Network::new(
        match args.model {
            NetworkModel::Constant => {
                boxed!(ConstantBandwidthNetworkModel::new(10., 0.1))
            }
            NetworkModel::Shared => {
                boxed!(SharedBandwidthNetworkModel::new(10., 0.1))
            }
        },
        sim.create_context("net"),
    );
    net.add_node("node1", boxed!(ConstantBandwidthNetworkModel::new(1000., 0.)));
    net.add_node("node2", boxed!(ConstantBandwidthNetworkModel::new(1000., 0.)));
    let net_rc = rc!(refcell!(net));
    sim.add_handler("net", net_rc.clone());

    // add other simulation components (sender and two receivers)
    let sender = DataSender::new(net_rc.clone(), sim.create_context("sender"));
    let sender_rc = rc!(refcell!(sender));
    let sender_id = sim.add_handler("sender", sender_rc.clone());
    let local_receiver = DataReceiver::new(net_rc.clone(), sim.create_context("local receiver"));
    let local_receiver_id = sim.add_handler("local receiver", rc!(refcell!(local_receiver)));
    let remote_receiver = DataReceiver::new(net_rc.clone(), sim.create_context("remote receiver"));
    let remote_receiver_id = sim.add_handler("remote receiver", rc!(refcell!(remote_receiver)));

    // bind sender and receivers to network nodes
    net_rc.borrow_mut().set_location(sender_id, "node1");
    net_rc.borrow_mut().set_location(local_receiver_id, "node1");
    net_rc.borrow_mut().set_location(remote_receiver_id, "node2");

    // send data
    sender_rc.borrow().send_data(1000., local_receiver_id);
    sender_rc.borrow().send_data(1000., remote_receiver_id);

    // send more data after 10 seconds
    sim.step_for_duration(10.);
    sender_rc.borrow().send_data(500., local_receiver_id);
    sender_rc.borrow().send_data(500., remote_receiver_id);

    sim.step_until_no_events();
}

// Data Sender ---------------------------------------------------------------------------------------------------------

pub struct DataSender {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl DataSender {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }

    pub fn send_data(&self, size: f64, dst: Id) {
        let id = self.net.borrow_mut().transfer_data(self.ctx.id(), dst, size, dst);
        log_info!(self.ctx, "Started data transfer {} of size {} to {}", id, size, dst);
    }
}

// Data Receiver -------------------------------------------------------------------------------------------------------

impl EventHandler for DataSender {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            MessageDelivered { msg } => {
                log_info!(self.ctx, "Received message '{}' from {}", msg.data, msg.src);
            }
        })
    }
}

pub struct DataReceiver {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl DataReceiver {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for DataReceiver {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            DataTransferCompleted { dt } => {
                log_info!(
                    self.ctx,
                    "Completed data transfer {} of size {} from {}",
                    dt.id,
                    dt.size,
                    dt.src
                );
                // send message back to the sender to confirm receipt of the data
                self.net
                    .borrow_mut()
                    .send_msg(format!("Ack data transfer {}", dt.id), self.ctx.id(), dt.src);
            }
        })
    }
}
