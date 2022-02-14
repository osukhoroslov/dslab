mod common;
mod compute;
mod master;
mod network;
mod storage;
mod task;
mod worker;

use core2::simulator::Simulator;
use rand::prelude::*;
use rand_pcg::Pcg64;
use std::time::Instant;
use sugars::{rc, refcell};

use crate::common::Start;
use crate::compute::Compute;
use crate::master::{Master, ReportStatus};
use crate::network::Network;
use crate::storage::Storage;
use crate::task::TaskRequest;
use crate::worker::Worker;

fn main() {
    env_logger::init();

    // params
    let host_count = 1000;
    let latency = 0.5;
    let storage_bandwidth = 2000;
    let network_bandwidth = 1000;
    let task_count = 10000;
    let seed = 123;

    let mut sim = Simulator::new(seed);
    let mut rand = Pcg64::seed_from_u64(seed);
    // admin context for starting master and workers
    let mut admin = sim.create_context("admin");
    // client context for submitting tasks
    let mut client = sim.create_context("client");

    // create network and add hosts
    let net = rc!(refcell!(Network::new(
        latency,
        network_bandwidth,
        sim.create_context("net")
    )));
    sim.add_handler("net", net.clone());
    for i in 0..host_count {
        net.borrow_mut().add_host(&format!("host{}", i));
    }
    let hosts = net.borrow().get_hosts();

    // create and start master on host0
    let host = &hosts[0];
    let master_id = &format!("/{}/master", host);
    let master = Master::new(net.clone(), sim.create_context(master_id));
    sim.add_handler(master_id, rc!(refcell!(master)));
    net.borrow_mut().set_location(master_id, host);
    admin.emit_now(Start {}, master_id);

    // create and start workers
    for i in 0..host_count {
        let host = &hosts[i];
        let compute_id = format!("/{}/compute", host);
        let compute = Compute::new(
            rand.gen_range(1..10),
            rand.gen_range(1..4),
            rand.gen_range(1..4) * 1024,
            sim.create_context(&compute_id),
        );
        let storage_id = format!("/{}/disk", host);
        let storage = Storage::new(storage_bandwidth, storage_bandwidth, sim.create_context(&storage_id));
        let worker_id = &format!("/{}/worker", host);
        let worker = Worker::new(
            compute,
            storage,
            net.clone(),
            master_id.to_string(),
            sim.create_context(worker_id),
        );
        sim.add_handler(worker_id, rc!(refcell!(worker)));
        net.borrow_mut().set_location(worker_id, host);
        admin.emit_now(Start {}, worker_id);
    }

    // let workers to register on master
    sim.step_for_duration(1.);

    // submit tasks
    for i in 0..task_count {
        let task = TaskRequest {
            id: i,
            flops: rand.gen_range(10..100),
            cpus: 1,
            memory: rand.gen_range(1..8) * 128,
            input_size: rand.gen_range(100..1000),
            output_size: rand.gen_range(10..100),
        };
        client.emit_now(task, master_id);
    }

    // enable status reporting
    admin.emit_now(ReportStatus {}, master_id);

    // run until completion
    let t = Instant::now();
    sim.step_until_no_events();
    println!(
        "Processed {} events in {:.2?} ({:.0} events/sec)",
        sim.event_count(),
        t.elapsed(),
        sim.event_count() as f64 / t.elapsed().as_secs_f64()
    );
}
