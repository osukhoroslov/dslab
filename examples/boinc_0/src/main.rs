mod client;
mod common;
mod job;
mod job_generator;
mod server;

use std::io::Write;
use std::time::Instant;

use clap::Parser;
use env_logger::Builder;
use rand::prelude::*;
use rand_pcg::Pcg64;
use sugars::{boxed, rc, refcell};

use dslab_compute::multicore::{Compute, CoresDependency};
use dslab_core::simulation::Simulation;
use dslab_network::models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel};
use dslab_network::{Network, NetworkModel};
use dslab_storage::disk::DiskBuilder;

use crate::client::Client;
use crate::common::Start;
use crate::job::JobRequest;
use crate::job_generator::NoMoreJobs;
use crate::server::Server;

/// Server-clients example
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of hosts
    #[clap(long, default_value_t = 100)]
    host_count: usize,

    /// Number of jobs
    #[clap(long, default_value_t = 100000)]
    job_count: u64,

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
    let host_count = 2;
    let local_latency = 0.0;
    let local_bandwidth = 10000;
    let network_latency = 0.5;
    let network_bandwidth = 1000;
    let disk_capacity = 1000;
    let disk_read_bandwidth = 2000.;
    let disk_write_bandwidth = 2000.;
    let job_count = 10;
    let use_shared_network = args.use_shared_network;
    let seed = 123;

    let mut sim = Simulation::new(seed);
    let mut rand = Pcg64::seed_from_u64(seed);
    // admin context for starting server and clients
    let admin = sim.create_context("admin");
    // client context for submitting jobs
    let manager = sim.create_context("manager");

    // create network and add hosts
    let network_model: Box<dyn NetworkModel> = if use_shared_network {
        boxed!(SharedBandwidthNetworkModel::new(
            network_bandwidth as f64,
            network_latency
        ))
    } else {
        boxed!(ConstantBandwidthNetworkModel::new(
            network_bandwidth as f64,
            network_latency
        ))
    };
    let network = rc!(refcell!(Network::new(network_model, sim.create_context("net"))));
    sim.add_handler("net", network.clone());
    for i in 0..host_count {
        network.borrow_mut().add_node(
            &format!("host{}", i),
            Box::new(SharedBandwidthNetworkModel::new(local_bandwidth as f64, local_latency)),
        );
    }
    let hosts = network.borrow().get_nodes();

    // create and start server on host0
    let host = &hosts[0];
    let server_name = &format!("{}::server", host);
    let server = rc!(refcell!(Server::new(network.clone(), sim.create_context(server_name))));
    let server_id = sim.add_handler(server_name, server.clone());
    network.borrow_mut().set_location(server_id, host);
    admin.emit_now(Start {}, server_id);

    // create and start clients
    for host in hosts.iter() {
        // compute
        let compute_name = format!("{}::compute", host);
        let compute = rc!(refcell!(Compute::new(
            rand.gen_range(1..=10) as f64,
            rand.gen_range(1..=8),
            rand.gen_range(1..=4) * 1024,
            sim.create_context(&compute_name),
        )));
        sim.add_handler(compute_name, compute.clone());
        // disk
        let disk_name = format!("{}::disk", host);
        let disk = rc!(refcell!(DiskBuilder::simple(
            disk_capacity,
            disk_read_bandwidth,
            disk_write_bandwidth
        )
        .build(sim.create_context(&disk_name))));
        sim.add_handler(disk_name, disk.clone());

        let client_name = &format!("{}::client", host);
        let client = Client::new(
            compute,
            disk,
            network.clone(),
            server_id,
            sim.create_context(client_name),
        );
        let client_id = sim.add_handler(client_name, rc!(refcell!(client)));
        network.borrow_mut().set_location(client_id, host);
        admin.emit_now(Start {}, client_id);
    }

    // submit jobs
    for i in 0..job_count {
        let job = JobRequest {
            id: i,
            flops: rand.gen_range(100..=1000) as f64,
            memory: rand.gen_range(1..=8) * 128,
            min_cores: 1,
            max_cores: 1,
            cores_dependency: CoresDependency::Linear,
            input_size: rand.gen_range(100..=1000),
            output_size: rand.gen_range(10..=100),
        };
        manager.emit_now(job, server_id);
    }
    manager.emit_now(NoMoreJobs {}, server_id);

    // run until completion
    let t = Instant::now();
    sim.step_until_no_events();
    let duration = t.elapsed().as_secs_f64();
    println!(
        "Processed {} jobs on {} hosts in {:.2}s ({:.2} jobs/s)",
        job_count,
        host_count,
        sim.time(),
        job_count as f64 / sim.time()
    );
    println!("Elapsed time: {:.2}s", duration);
    println!("Scheduling time: {:.2}s", server.borrow().scheduling_time);
    println!("Simulation speedup: {:.2}", sim.time() / duration);
    println!(
        "Processed {} events in {:.2?}s ({:.0} events/s)",
        sim.event_count(),
        duration,
        sim.event_count() as f64 / duration
    );
}
