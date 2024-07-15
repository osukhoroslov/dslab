use std::cell::RefCell;
use std::rc::Rc;

use rstest::rstest;
use serde::Serialize;

use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::simulation::Simulation;
use simcore::EPSILON;

use dslab_network::models::{ConstantBandwidthNetworkModel, TopologyAwareNetworkModel};
use dslab_network::routing::{RoutingAlgorithm, ShortestPathDijkstra, ShortestPathFloydWarshall};
use dslab_network::{DataTransferCompleted, Link, Network};

#[derive(Clone, Copy)]
enum RoutingImpl {
    Dijkstra,
    FloydWarshall,
}

fn assert_float_eq(x: f64, y: f64, eps: f64) {
    assert!(
        (x - y).abs() < eps || (x.max(y) - x.min(y)) / x.min(y) < eps,
        "Values do not match: {:.15} vs {:.15}",
        x,
        y
    );
}

#[derive(Clone, Serialize)]
pub struct Start {
    size: f64,
    receiver_id: Id,
}

pub struct Node {
    net: Rc<RefCell<Network>>,
    ctx: SimulationContext,
}

impl Node {
    pub fn new(net: Rc<RefCell<Network>>, ctx: SimulationContext) -> Self {
        Self { net, ctx }
    }
}

impl EventHandler for Node {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start { size, receiver_id } => {
                self.net
                    .borrow_mut()
                    .transfer_data(self.ctx.id(), receiver_id, size, receiver_id);
            }
            DataTransferCompleted { dt: _ } => {}
        })
    }
}

fn run_link_test(
    link: Link,
    bidirectional: bool,
    full_mesh_optimization: bool,
    routing: RoutingImpl,
    lr_transfers: usize,
    rl_transfers: usize,
) -> f64 {
    // two nodes, one link (or two directional), [lr_transfers] transfers from 1 to 2, [rl_transfers] from 2 to 1

    let mut sim = Simulation::new(123);

    let routing: Box<dyn RoutingAlgorithm> = match routing {
        RoutingImpl::Dijkstra => Box::new(ShortestPathDijkstra::default()),
        RoutingImpl::FloydWarshall => Box::new(ShortestPathFloydWarshall::default()),
    };
    let network_model = TopologyAwareNetworkModel::new()
        .with_routing(routing)
        .with_full_mesh_optimization(full_mesh_optimization);
    let mut network = Network::new(Box::new(network_model), sim.create_context("net"));

    network.add_node("host1", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("host2", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));

    if bidirectional {
        network.add_link("host1", "host2", link);
    } else {
        network.add_full_duplex_link("host1", "host2", link);
    }

    network.init_topology();

    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());

    let node1 = Node::new(network_rc.clone(), sim.create_context("node1"));
    let node1_id = sim.add_handler("node1", Rc::new(RefCell::new(node1)));

    let node2 = Node::new(network_rc.clone(), sim.create_context("node2"));
    let node2_id = sim.add_handler("node2", Rc::new(RefCell::new(node2)));

    network_rc.borrow_mut().set_location(node1_id, "host1");
    network_rc.borrow_mut().set_location(node2_id, "host2");

    let client = sim.create_context("client");

    for _ in 0..lr_transfers {
        client.emit_now(
            Start {
                size: 1000.0,
                receiver_id: node2_id,
            },
            node1_id,
        );
    }
    for _ in 0..rl_transfers {
        client.emit_now(
            Start {
                size: 1000.0,
                receiver_id: node1_id,
            },
            node2_id,
        );
    }

    sim.step_until_no_events();
    sim.time()
}

#[rstest]
fn test_links(
    #[values(false, true)] full_mesh_optimization: bool,
    #[values(RoutingImpl::Dijkstra, RoutingImpl::FloydWarshall)] routing: RoutingImpl,
    #[values(1, 2, 3, 4, 5)] lr_transfers: usize,
    #[values(1, 2, 3, 4, 5)] rl_transfers: usize,
) {
    assert_float_eq(
        run_link_test(
            Link::shared(100., 0.),
            true,
            full_mesh_optimization,
            routing,
            lr_transfers,
            rl_transfers,
        ),
        10. * (lr_transfers + rl_transfers) as f64,
        EPSILON,
    );
    assert_float_eq(
        run_link_test(
            Link::shared(100., 0.),
            false,
            full_mesh_optimization,
            routing,
            lr_transfers,
            rl_transfers,
        ),
        10. * lr_transfers.max(rl_transfers) as f64,
        EPSILON,
    );
    assert_float_eq(
        run_link_test(
            Link::non_shared(100., 0.),
            true,
            full_mesh_optimization,
            routing,
            lr_transfers,
            rl_transfers,
        ),
        10.,
        EPSILON,
    );
    assert_float_eq(
        run_link_test(
            Link::non_shared(100., 0.),
            false,
            full_mesh_optimization,
            routing,
            lr_transfers,
            rl_transfers,
        ),
        10.,
        EPSILON,
    );
}

#[rstest]
fn test_triangle(#[values(RoutingImpl::Dijkstra, RoutingImpl::FloydWarshall)] routing: RoutingImpl) {
    let mut sim = Simulation::new(123);

    let routing: Box<dyn RoutingAlgorithm> = match routing {
        RoutingImpl::Dijkstra => Box::new(ShortestPathDijkstra::default()),
        RoutingImpl::FloydWarshall => Box::new(ShortestPathFloydWarshall::default()),
    };
    let network_model = Box::new(TopologyAwareNetworkModel::new().with_routing(routing));
    let mut network = Network::new(network_model, sim.create_context("net"));

    network.add_node("host1", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("host2", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("host3", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));

    network.add_unidirectional_link("host1", "host2", Link::shared(100., 0.1));
    network.add_unidirectional_link("host2", "host3", Link::shared(100., 0.1));
    network.add_unidirectional_link("host3", "host1", Link::shared(100., 0.1));

    network.init_topology();
    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());

    let node1 = Node::new(network_rc.clone(), sim.create_context("node1"));
    let node1_id = sim.add_handler("node1", Rc::new(RefCell::new(node1)));
    let node2 = Node::new(network_rc.clone(), sim.create_context("node2"));
    let node2_id = sim.add_handler("node2", Rc::new(RefCell::new(node2)));
    let node3 = Node::new(network_rc.clone(), sim.create_context("node3"));
    let node3_id = sim.add_handler("node3", Rc::new(RefCell::new(node3)));

    network_rc.borrow_mut().set_location(node1_id, "host1");
    network_rc.borrow_mut().set_location(node2_id, "host2");
    network_rc.borrow_mut().set_location(node3_id, "host3");

    let client = sim.create_context("client");

    client.emit_now(
        Start {
            size: 1000.0,
            receiver_id: node3_id,
        },
        node1_id,
    );
    client.emit_now(
        Start {
            size: 1000.0,
            receiver_id: node1_id,
        },
        node2_id,
    );
    client.emit_now(
        Start {
            size: 1000.0,
            receiver_id: node2_id,
        },
        node3_id,
    );

    sim.step_until_no_events();

    assert_float_eq(sim.time(), 20.2, EPSILON);
}

#[rstest]
fn test_diamond(#[values(RoutingImpl::Dijkstra, RoutingImpl::FloydWarshall)] routing: RoutingImpl) {
    let mut sim = Simulation::new(123);

    let routing: Box<dyn RoutingAlgorithm> = match routing {
        RoutingImpl::Dijkstra => Box::new(ShortestPathDijkstra::default()),
        RoutingImpl::FloydWarshall => Box::new(ShortestPathFloydWarshall::default()),
    };
    let network_model = Box::new(TopologyAwareNetworkModel::new().with_routing(routing));
    let mut network = Network::new(network_model, sim.create_context("net"));

    network.add_node("input", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("mid1", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("mid2", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));
    network.add_node("output", Box::new(ConstantBandwidthNetworkModel::new(100.0, 0.0)));

    network.add_unidirectional_link("input", "mid1", Link::shared(1000., 1.));
    network.add_unidirectional_link("mid1", "output", Link::shared(1000., 0.1));
    network.add_unidirectional_link("input", "mid2", Link::shared(100., 0.1));
    network.add_unidirectional_link("mid2", "output", Link::shared(100., 0.1));

    network.init_topology();
    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());

    let input = Node::new(network_rc.clone(), sim.create_context("input"));
    let input = sim.add_handler("input", Rc::new(RefCell::new(input)));
    let output = Node::new(network_rc.clone(), sim.create_context("output"));
    let output = sim.add_handler("output", Rc::new(RefCell::new(output)));

    network_rc.borrow_mut().set_location(input, "input");
    network_rc.borrow_mut().set_location(output, "output");

    let client = sim.create_context("client");

    client.emit_now(
        Start {
            size: 1000.0,
            receiver_id: output,
        },
        input,
    );

    sim.step_until_no_events();

    assert_float_eq(sim.time(), 10.2, EPSILON);
}
