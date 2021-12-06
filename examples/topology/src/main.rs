use std::fmt::Debug;
use sugars::{rc, refcell};

use core::actor::{Actor, ActorContext, ActorId, Event};
use core::sim::Simulation;
use core::{cast, match_event};

// SYSTEM ACTORS ///////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CompRequest {
    task_id: u64,
}

#[derive(Debug)]
pub struct CompStarted {
    task_id: u64,
}

#[derive(Debug)]
pub struct CompFinished {
    task_id: u64,
}

pub struct ComputeActor {}

impl Actor for ComputeActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            CompRequest { task_id } => {
                println!("{} [{}] comp request {}", ctx.time(), ctx.id, task_id);
                ctx.emit(CompStarted { task_id: *task_id }, from.clone(), 0.);
                ctx.emit(CompFinished { task_id: *task_id }, from.clone(), 10.);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct ReadRequest {}

#[derive(Debug)]
pub struct ReadCompleted {}

#[derive(Debug)]
pub struct WriteRequest {}

#[derive(Debug)]
pub struct WriteCompleted {}

pub struct StorageActor {}

impl Actor for StorageActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {}

    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct MessageSend {
    msg: String,
    dest: ActorId,
}

#[derive(Debug)]
pub struct MessageReceive {
    msg: String,
    source: ActorId,
}

pub struct NetworkActor {}

impl Actor for NetworkActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            MessageSend { msg, dest } => {
                ctx.emit(
                    MessageReceive {
                        msg: (*msg).clone(),
                        source: from,
                    },
                    (*dest).clone(),
                    1.,
                );
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// EVENTS //////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Start {}

#[derive(Debug)]
pub struct NodeRegister {}

#[derive(Debug)]
pub struct TaskSubmitted {
    task_id: u64,
}

#[derive(Debug)]
pub struct TaskAssigned {
    task_id: u64,
}

#[derive(Debug)]
pub struct TaskCompleted {
    task_id: u64,
}

// NODE MANAGER ////////////////////////////////////////////////////////////////////////////////////

pub struct NodeManager {
    compute: ActorId,
    storage: ActorId,
    network: ActorId,
    resource_manager: ActorId,
}

impl NodeManager {
    pub fn new(compute: ActorId, storage: ActorId, network: ActorId, resource_manager: ActorId) -> Self {
        Self {
            compute,
            storage,
            network,
            resource_manager,
        }
    }
}

impl Actor for NodeManager {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
                // ctx.emit(NodeRegister {}, self.resource_manager.clone(), 0.);
                ctx.emit(
                    MessageSend {
                        msg: "NodeRegister".to_string(),
                        dest: self.resource_manager.clone(),
                    },
                    self.network.clone(),
                    0.,
                );
                // TODO: wrap network actor to provide interface like this
                // self.network.send(NodeRegister {}, self.resource_manager.clone());
            }
            TaskAssigned { task_id } => {
                println!("{} [{}] assigned task {}", ctx.time(), ctx.id, task_id);
                ctx.emit(CompRequest { task_id: *task_id }, self.compute.clone(), 0.);
            }
            CompStarted { task_id } => {
                println!("{} [{}] started task {}", ctx.time(), ctx.id, task_id);
            }
            CompFinished { task_id } => {
                println!("{} [{}] completed task {}", ctx.time(), ctx.id, task_id);
                ctx.emit(TaskCompleted { task_id: *task_id }, self.resource_manager.clone(), 3.);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// RESOURCE MANAGER ////////////////////////////////////////////////////////////////////////////////

pub struct ResourceManager {
    network: ActorId,
    nodes: Vec<ActorId>,
}

impl ResourceManager {
    pub fn new(network: ActorId) -> Self {
        Self {
            network,
            nodes: Vec::new(),
        }
    }
}

impl Actor for ResourceManager {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                println!("{} [{}] started", ctx.time(), ctx.id);
            }
            // NodeRegister {} => {
            //     println!("{} [{}] registered node {}", ctx.time(), ctx.id, from);
            //     self.nodes.push(from);
            // }
            // TODO: support receiving plain events from the network instead of generic MessageReceive?
            MessageReceive { msg, source } => {
                match msg.as_str() {
                    "NodeRegister" => {
                        println!("{} [{}] registered node {}", ctx.time(), ctx.id, source);
                        self.nodes.push((*source).clone());
                    }
                    _ => {}
                }
            }
            TaskSubmitted { task_id } => {
                println!("{} [{}] new task {}", ctx.time(), ctx.id, task_id);
                let node = self.nodes.first().unwrap();
                ctx.emit(TaskAssigned { task_id: *task_id }, node.clone(), 0.);
            }
            TaskCompleted { task_id } => {
                println!("{} [{}] task {} completed", ctx.time(), ctx.id, task_id);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

// MAIN ////////////////////////////////////////////////////////////////////////////////////////////

fn main() {
    let mut sim = Simulation::new(123);
    let mut resource_manager = ActorId::from("null");

    for i in 0..10 {
        let node_path = format!("/rack{}/node{}", i / 5, i);
        let network = sim.add_actor(&format!("{}/network", node_path), rc!(refcell!(NetworkActor {})));
        if i == 0 {
            resource_manager = sim.add_actor(
                &format!("{}/resource-manager", node_path),
                rc!(refcell!(ResourceManager::new(network))),
            );
            sim.add_event(Start {}, ActorId::from("root"), resource_manager.clone(), 0.);
        } else {
            let compute = sim.add_actor(&format!("{}/compute", node_path), rc!(refcell!(ComputeActor {})));
            let storage = sim.add_actor(&format!("{}/storage", node_path), rc!(refcell!(StorageActor {})));
            let node_manager = sim.add_actor(
                &format!("{}/node-manager", node_path),
                rc!(refcell!(NodeManager::new(
                    compute,
                    storage,
                    network,
                    resource_manager.clone()
                ))),
            );
            sim.add_event(Start {}, ActorId::from("root"), node_manager, 0.);
        }
    }

    sim.step_until_no_events();

    sim.add_event(
        TaskSubmitted { task_id: 1 },
        ActorId::from("root"),
        resource_manager.clone(),
        0.,
    );

    sim.step_until_no_events();
}
