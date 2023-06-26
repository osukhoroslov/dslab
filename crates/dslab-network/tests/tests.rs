use std::cell::RefCell;
use std::rc::Rc;

use serde::Serialize;

use dslab_core::cast;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::EPSILON;
use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::DataTransferCompleted;
use dslab_network::network::Network;
use dslab_network::topology::Topology;
use dslab_network::topology_model::TopologyNetwork;
use dslab_network::topology_resolver::TopologyResolveType;
use dslab_network::topology_structures::Link;

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
            DataTransferCompleted { data: _ } => {}
        })
    }
}

fn run_link_test(
    link: Link,
    bidirectional: bool,
    full_mesh_optimization: bool,
    resolve_type: TopologyResolveType,
    lr_transfers: usize,
    rl_transfers: usize,
) -> f64 {
    // two nodes, one link (or two directional), [lr_transfers] transfers from 1 to 2, [rl_transfers] from 2 to 1

    let mut sim = Simulation::new(123);
    let mut topology = Topology::new().with_resolve_type(resolve_type);

    topology.add_node("host1", Box::new(ConstantBandwidthNetwork::new(100.0, 0.0)));
    topology.add_node("host2", Box::new(ConstantBandwidthNetwork::new(100.0, 0.0)));

    if bidirectional {
        topology.add_link("host1", "host2", link, true);
    } else {
        topology.add_link("host1", "host2", link, false);
        topology.add_link("host2", "host1", link, false);
    }

    topology.init();

    let topology_rc = Rc::new(RefCell::new(topology));
    let network_model = Rc::new(RefCell::new(
        TopologyNetwork::new(topology_rc.clone()).with_full_mesh_optimization(full_mesh_optimization),
    ));
    let network = Network::new_with_topology(network_model, topology_rc.clone(), sim.create_context("net"));

    let network_rc = Rc::new(RefCell::new(network));
    sim.add_handler("net", network_rc.clone());

    let node1 = Node::new(network_rc.clone(), sim.create_context("node1"));
    let node1_id = sim.add_handler("node1", Rc::new(RefCell::new(node1)));

    let node2 = Node::new(network_rc.clone(), sim.create_context("node2"));
    let node2_id = sim.add_handler("node2", Rc::new(RefCell::new(node2)));

    topology_rc.borrow_mut().set_location(node1_id, "host1");
    topology_rc.borrow_mut().set_location(node2_id, "host2");

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

#[test]
fn test_links() {
    for full_mesh_optimization in [false, true] {
        for resolve_type in [TopologyResolveType::Dijkstra, TopologyResolveType::FloydWarshall] {
            for lr_transfers in 1..=5 {
                for rl_transfers in 1..=5 {
                    println!("Testing full_mesh_optimization={full_mesh_optimization:?}, {resolve_type:?}, transfers: {lr_transfers}, {rl_transfers}");
                    assert_float_eq(
                        run_link_test(
                            Link::shared(100., 0.),
                            true,
                            full_mesh_optimization,
                            resolve_type,
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
                            resolve_type,
                            lr_transfers,
                            rl_transfers,
                        ),
                        10. * lr_transfers.max(rl_transfers) as f64,
                        EPSILON,
                    );
                    assert_float_eq(
                        run_link_test(
                            Link::fatpipe(100., 0.),
                            true,
                            full_mesh_optimization,
                            resolve_type,
                            lr_transfers,
                            rl_transfers,
                        ),
                        10.,
                        EPSILON,
                    );
                    assert_float_eq(
                        run_link_test(
                            Link::fatpipe(100., 0.),
                            false,
                            full_mesh_optimization,
                            resolve_type,
                            lr_transfers,
                            rl_transfers,
                        ),
                        10.,
                        EPSILON,
                    );
                }
            }
        }
    }
}
