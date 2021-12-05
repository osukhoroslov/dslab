use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use compute::computation::Computation;
use compute::singlecore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use core::sim::Simulation;

pub mod workflow;
use workflow::*;

pub struct TaskActor {
    workflow: Workflow,
    available_actors: Vec<ActorId>,
    scheduled_tasks: BTreeMap<usize, usize>,
    history: BTreeMap<String, String>,
}

impl TaskActor {
    pub fn new(workflow: Workflow, compute_actors: Vec<ActorId>) -> Self {
        Self {
            workflow: workflow,
            available_actors: compute_actors,
            scheduled_tasks: BTreeMap::new(),
            history: BTreeMap::new(),
        }
    }
}

impl TaskActor {
    fn schedule_ready(&mut self, ctx: &mut ActorContext) {
        for &task in self.workflow.ready_tasks.iter() {
            if self.scheduled_tasks.contains_key(&task) {
                continue;
            }
            if let Some(actor_id) = self.available_actors.pop() {
                ctx.emit(
                    CompRequest {
                        computation: self.workflow.tasks[task].clone(),
                    },
                    actor_id,
                    0.,
                );
                self.scheduled_tasks.insert(task, ctx.time().round() as usize);
            } else {
                break;
            }
        }
    }
}

impl Actor for TaskActor {
    fn on(&mut self, event: Box<dyn Event>, from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                println!("{} [{}] received Start from {}", ctx.time(), ctx.id, from);
                self.schedule_ready(ctx);
            },
            CompStarted { computation: _ } => {},
            CompFinished { computation } => {
                println!(
                    "{} [{}] received CompFinished from {} for {:?}",
                    ctx.time(),
                    ctx.id,
                    from,
                    computation
                );

                let start_time = self.scheduled_tasks.get(&(computation.id as usize)).unwrap();
                let end_time = ctx.time().round() as usize;
                let entry = self.history.entry(from.to()).or_insert("".to_string());
                *entry += &String::from_utf8(vec![b' '; start_time - entry.len()]).unwrap();
                *entry += &format!("[{:.^width$}]", computation.id, width = end_time - start_time - 2);

                self.workflow.mark_completed(computation.id as usize);
                self.available_actors.push(from);
                self.schedule_ready(ctx);

                if self.workflow.completed() {
                    for (actor, hist) in self.history.iter() {
                        println!("{}: {}", actor, hist);
                    }
                }
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}

fn diamond_example() {
    let mut g = Workflow::new();
    g.add_task(Computation::new(9, 512, 0));
    g.add_task(Computation::new(9, 512, 1));
    g.add_task(Computation::new(9, 512, 2));
    g.add_task(Computation::new(9, 512, 3));
    g.add_edge(Edge::new(0, 3, 1));
    g.add_edge(Edge::new(0, 2, 1));
    g.add_edge(Edge::new(2, 1, 1));
    g.add_edge(Edge::new(3, 1, 1));
    eprintln!("ready tasks: {:?}", g.ready_tasks);
    eprintln!("topsort: {:?}", g.topsort());
    eprintln!("validate: {}", g.validate());

    let mut sim = Simulation::new(123);
    sim.add_actor("compute1", Rc::new(RefCell::new(ComputeActor::new(1, 1024))));
    sim.add_actor("compute2", Rc::new(RefCell::new(ComputeActor::new(1, 1024))));
    sim.add_actor("compute3", Rc::new(RefCell::new(ComputeActor::new(1, 1024))));
    sim.add_actor(
        "scheduler",
        Rc::new(RefCell::new(TaskActor::new(
            g,
            vec![
                ActorId::from("compute1"),
                ActorId::from("compute2"),
                ActorId::from("compute3"),
            ],
        ))),
    );
    sim.add_event(Start {}, ActorId::from("0"), ActorId::from("scheduler"), 0.);
    sim.step_until_no_events();
}

fn main() {
    diamond_example();
}
