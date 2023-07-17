use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;
use dslab_network::topology::Topology;
use dslab_network::topology_structures::Link;

pub fn make_full_mesh_topology(host_count: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..host_count {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
    }

    for i in 0..host_count {
        for j in 0..i {
            topology.add_link(
                &format!("host_{}", i),
                &format!("host_{}", j),
                Link::shared(1000., 1e-4),
            );
        }
    }
    topology
}

pub fn make_star_topology(host_count: usize) -> Topology {
    let mut topology = Topology::new();

    let switch_name = "switch".to_string();
    topology.add_node(&switch_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));

    for i in 0..host_count {
        let host_name = format!("host_{}", i);
        topology.add_node(&host_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
        topology.add_link(&host_name, &switch_name, Link::shared(1000., 1e-4));
    }
    topology
}

pub fn make_tree_topology(star_count: usize, hosts_per_star: usize) -> Topology {
    let mut topology = Topology::new();

    let root_switch_name = "root_switch".to_string();
    topology.add_node(&root_switch_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));

    let downlink_bw = 1000.;
    for i in 0..star_count {
        let switch_name = format!("switch_{}", i);
        topology.add_node(&switch_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
        topology.add_link(
            &root_switch_name,
            &switch_name,
            Link::shared(downlink_bw * hosts_per_star as f64, 1e-4),
        );

        for j in 0..hosts_per_star {
            let host_name = format!("host_{}_{}", i, j);
            topology.add_node(&host_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
            topology.add_link(&host_name, &switch_name, Link::shared(downlink_bw, 1e-4));
        }
    }
    topology
}

pub fn make_fat_tree_topology(l2_switch_count: usize, l1_switch_count: usize, hosts_per_switch: usize) -> Topology {
    let mut topology = Topology::new();

    for i in 0..l2_switch_count {
        let switch_name = format!("l2_switch_{}", i);
        topology.add_node(&switch_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
    }

    let downlink_bw = 1000.;
    let uplink_bw = downlink_bw * hosts_per_switch as f64 / l2_switch_count as f64;

    for i in 0..l1_switch_count {
        let switch_name = format!("l1_switch_{}", i);
        topology.add_node(&switch_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));

        for j in 0..hosts_per_switch {
            let host_name = format!("host_{}_{}", i, j);
            topology.add_node(&host_name, Box::new(SharedBandwidthNetwork::new(1e+5, 0.)));
            topology.add_link(&switch_name, &host_name, Link::shared(downlink_bw, 1e-4));
        }

        for j in 0..l2_switch_count {
            topology.add_link(&switch_name, &format!("l2_switch_{}", j), Link::shared(uplink_bw, 1e-4));
        }
    }
    topology
}
