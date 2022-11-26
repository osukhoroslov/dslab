use env_logger::Builder;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use serde::Serialize;
use sugars::{rc, refcell};

use dslab_compute::multicore::*;
use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::simulation::Simulation;
use dslab_core::{cast, log_error, log_info};

#[derive(Serialize)]
pub struct Start {}

#[derive(Serialize)]
pub struct Deallocate {}

pub struct Task {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    flops: u64,
    memory: u64,
    min_cores: u32,
    max_cores: u32,
    cores_dependency: CoresDependency,
    ctx: SimulationContext,
}

impl Task {
    pub fn new(
        compute: Rc<RefCell<Compute>>,
        flops: u64,
        memory: u64,
        min_cores: u32,
        max_cores: u32,
        cores_dependency: CoresDependency,
        ctx: SimulationContext,
    ) -> Self {
        Self {
            id: ctx.id(),
            compute,
            flops,
            memory,
            min_cores,
            max_cores,
            cores_dependency,
            ctx,
        }
    }
}

impl EventHandler for Task {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                println!("{} [{}] received Start from {}", self.ctx.time(), self.id, event.src);
                self.compute.borrow_mut().run(
                    self.flops,
                    self.memory,
                    self.min_cores,
                    self.max_cores,
                    self.cores_dependency.clone(),
                    self.id,
                );
            }
            CompStarted { id, cores } => {
                println!(
                    "{} [{}] received CompStarted from {} for {:?} on {} cores",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id,
                    cores
                );
            }
            CompFinished { id } => {
                println!(
                    "{} [{}] received CompFinished from {} for {:?}",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id
                );
            }
            CompFailed { id, reason } => {
                println!(
                    "{} [{}] received CompFailed from {} for {:?} with reason {:?}",
                    self.ctx.time(),
                    self.id,
                    event.src,
                    id,
                    reason
                );
            }
        })
    }
}

pub struct Allocator {
    id: Id,
    compute: Rc<RefCell<Compute>>,
    allocation: Allocation,
    time: f64,
    ctx: SimulationContext,
}

impl Allocator {
    pub fn new(compute: Rc<RefCell<Compute>>, allocation: Allocation, time: f64, ctx: SimulationContext) -> Self {
        Self {
            id: ctx.id(),
            compute,
            allocation,
            time,
            ctx,
        }
    }
}

impl EventHandler for Allocator {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                log_info!(self.ctx, "received Start from {}", self.ctx.lookup_name(event.src));
                self.compute
                    .borrow_mut()
                    .allocate(self.allocation.cores, self.allocation.memory, self.id);
                self.ctx.emit_self(Deallocate {}, self.time);
            }
            Deallocate {} => {
                self.compute
                    .borrow_mut()
                    .deallocate(self.allocation.cores, self.allocation.memory, self.id);
            }
            AllocationFailed { id, reason } => {
                log_error!(
                    self.ctx,
                    "received AllocationFailed from {} for {:?} with reason {:?}",
                    self.ctx.lookup_name(event.src),
                    id,
                    reason
                );
            }
            DeallocationFailed { id, reason } => {
                log_error!(
                    self.ctx,
                    "received DeallocationFailed from {} for {:?} with reason {:?}",
                    self.ctx.lookup_name(event.src),
                    id,
                    reason
                );
            }
            AllocationSuccess { id } => {
                log_info!(
                    self.ctx,
                    "received AllocationSuccess from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
            }
            DeallocationSuccess { id } => {
                log_info!(
                    self.ctx,
                    "received DeallocationSuccess from {} for {:?}",
                    self.ctx.lookup_name(event.src),
                    id
                );
            }
        })
    }
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(123);

    let compute = rc!(refcell!(Compute::new(1, 10, 1024, sim.create_context("compute"))));
    sim.add_handler("compute", compute.clone());

    let task1 = Task::new(
        compute.clone(),
        100,
        512,
        2,
        6,
        CoresDependency::Linear,
        sim.create_context("task1"),
    );
    let task2 = Task::new(
        compute.clone(),
        100,
        512,
        4,
        10,
        CoresDependency::LinearWithFixed { fixed_part: 0.4 },
        sim.create_context("task2"),
    );
    let task3 = Task::new(
        compute.clone(),
        100,
        512,
        5,
        7,
        CoresDependency::Custom {
            func: |cores: u32| -> f64 {
                if cores == 7 {
                    2.0
                } else {
                    1.0
                }
            },
        },
        sim.create_context("task3"),
    );
    let task4 = Task::new(
        compute.clone(),
        100,
        512,
        15,
        20,
        CoresDependency::Linear,
        sim.create_context("task4"),
    );
    let task1_id = sim.add_handler("task1", rc!(refcell!(task1)));
    let task2_id = sim.add_handler("task2", rc!(refcell!(task2)));
    let task3_id = sim.add_handler("task3", rc!(refcell!(task3)));
    let task4_id = sim.add_handler("task4", rc!(refcell!(task4)));

    let allocator1 = Allocator::new(
        compute.clone(),
        Allocation::new(6, 100),
        10.,
        sim.create_context("allocator1"),
    );
    let allocator2 = Allocator::new(
        compute.clone(),
        Allocation::new(6, 100),
        20.,
        sim.create_context("allocator2"),
    );
    let allocator3 = Allocator::new(compute, Allocation::new(6, 100), 30., sim.create_context("allocator3"));
    let allocator1_id = sim.add_handler("allocator1", rc!(refcell!(allocator1)));
    let allocator2_id = sim.add_handler("allocator2", rc!(refcell!(allocator2)));
    let allocator3_id = sim.add_handler("allocator3", rc!(refcell!(allocator3)));

    let mut ctx = sim.create_context("root");
    ctx.emit(Start {}, task1_id, 0.);
    ctx.emit(Start {}, task2_id, 0.);
    ctx.emit(Start {}, task3_id, 1000.);
    ctx.emit(Start {}, task4_id, 2000.);
    ctx.emit(Start {}, allocator1_id, 5000.);
    ctx.emit(Start {}, allocator2_id, 5005.);
    ctx.emit(Start {}, allocator3_id, 6000.);

    sim.step_until_no_events();
}
