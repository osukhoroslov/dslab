mod common;
mod master;
mod task;
mod worker;

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use rand::prelude::*;
use rand_pcg::Pcg64;
use sugars::{rc, refcell};

use dslab_compute::multicore::{Compute, CoresDependency};
use dslab_core::simulation::Simulation;
use dslab_network::constant_bandwidth_model::ConstantBandwidthNetwork;
use dslab_network::model::NetworkModel;
use dslab_network::network::Network;
use dslab_network::shared_bandwidth_model::SharedBandwidthNetwork;
use dslab_storage::disk::Disk;

use crate::common::Start;
use crate::master::Master;
use crate::task::TaskRequest;
use crate::worker::Worker;

/// Master-workers example
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of hosts
    #[clap(long, default_value_t = 100)]
    host_count: usize,

    /// Number of tasks
    #[clap(long, default_value_t = 100000)]
    task_count: u64,

    /// Use shared network
    #[clap(long)]
    use_shared_network: bool,
}

fn main() {
    // logger
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    // params
    let args = Args::parse();
    let host_count = args.host_count;
    let local_latency = 0.0;
    let local_bandwidth = 10000;
    let network_latency = 0.5;
    let network_bandwidth = 1000;
    let disk_capacity = 1000;
    let disk_read_bandwidth = 2000;
    let disk_write_bandwidth = 2000;
    let task_count = args.task_count;
    let use_shared_network = args.use_shared_network;
    let seed = 123;

    let mut sim = Simulation::new(seed);
    let mut rand = Pcg64::seed_from_u64(seed);
    // admin context for starting master and workers
    let mut admin = sim.create_context("admin");
    // client context for submitting tasks
    let mut client = sim.create_context("client");

    // create network and add hosts
    let network_model: Rc<RefCell<dyn NetworkModel>> = if use_shared_network {
        rc!(refcell!(SharedBandwidthNetwork::new(
            network_bandwidth as f64,
            network_latency
        )))
    } else {
        rc!(refcell!(ConstantBandwidthNetwork::new(
            network_bandwidth as f64,
            network_latency
        )))
    };
    let network = rc!(refcell!(Network::new(network_model, sim.create_context("net"))));
    sim.add_handler("net", network.clone());
    for i in 0..host_count {
        network
            .borrow_mut()
            .add_node(&format!("host{}", i), local_bandwidth as f64, local_latency);
    }
    let hosts = network.borrow().get_nodes();

    // create and start master on host0
    let host = &hosts[0];
    let master_name = &format!("{}::master", host);
    let master = rc!(refcell!(Master::new(network.clone(), sim.create_context(master_name))));
    let master_id = sim.add_handler(master_name, master.clone());
    network.borrow_mut().set_location(master_id, host);
    admin.emit_now(Start {}, master_id);

    // create and start workers
    for host in hosts.iter() {
        // compute
        let compute_name = format!("{}::compute", host);
        let compute = rc!(refcell!(Compute::new(
            rand.gen_range(1..=10),
            rand.gen_range(1..=8),
            rand.gen_range(1..=4) * 1024,
            sim.create_context(&compute_name),
        )));
        sim.add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", host);
        let disk = Disk::new_simple(
            disk_capacity,
            disk_read_bandwidth,
            disk_write_bandwidth,
            sim.create_context(&disk_name),
        );
        let worker_name = &format!("{}::worker", host);
        let worker = Worker::new(
            compute,
            disk,
            network.clone(),
            master_id,
            sim.create_context(worker_name),
        );
        let worker_id = sim.add_handler(worker_name, rc!(refcell!(worker)));
        network.borrow_mut().set_location(worker_id, host);
        admin.emit_now(Start {}, worker_id);
    }

    // submit tasks
    for i in 0..task_count {
        let task = TaskRequest {
            id: i,
            flops: rand.gen_range(100..=1000),
            memory: rand.gen_range(1..=8) * 128,
            min_cores: 1,
            max_cores: 1,
            cores_dependency: CoresDependency::Linear,
            input_size: rand.gen_range(100..=1000),
            output_size: rand.gen_range(10..=100),
        };
        client.emit_now(task, master_id);
    }

    // run until completion
    let t = Instant::now();
    sim.step_until_no_events();
    let duration = t.elapsed().as_secs_f64();
    println!(
        "Processed {} tasks on {} hosts in {:.2}s ({:.2} tasks/s)",
        task_count,
        host_count,
        sim.time(),
        task_count as f64 / sim.time()
    );
    println!("Elapsed time: {:.2}s", duration);
    println!("Scheduling time: {:.2}s", master.borrow().scheduling_time);
    println!("Simulation speedup: {:.2}", sim.time() / duration);
    println!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        duration,
        sim.event_count() as f64 / duration
    );
}
