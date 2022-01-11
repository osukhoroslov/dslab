use crate::{TaskState, DAG};
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::match_event;
use std::collections::BTreeSet;

pub struct DAGRunner {
    dag: DAG,
    scheduled_tasks: BTreeSet<usize>,
}

impl DAGRunner {
    pub fn new(dag: DAG) -> Self {
        Self {
            dag,
            scheduled_tasks: BTreeSet::new(),
        }
    }

    pub fn start(&mut self, ctx: &mut ActorContext) {
        println!("{} [{}] started DAG execution", ctx.time(), ctx.id);
        self.schedule_ready_tasks(ctx);
    }

    pub fn on_task_completed(&mut self, task_id: usize, ctx: &mut ActorContext) {
        let task = self.dag.get_task(task_id);
        println!("{} [{}] task {} is completed", ctx.time(), ctx.id, task.name);
        self.dag.update_task_state(task_id, TaskState::Done);
        self.scheduled_tasks.remove(&task_id);

        self.schedule_ready_tasks(ctx);

        if self.dag.is_completed() {
            println!("{} [{}] completed DAG execution", ctx.time(), ctx.id);
        }
    }

    fn schedule_ready_tasks(&mut self, ctx: &mut ActorContext) {
        let mut scheduled = Vec::new();
        for t in self.dag.get_ready_tasks() {
            let task = self.dag.get_task(*t);
            ctx.emit_self(TaskCompleted { task_id: *t }, (task.flops / 10) as f64);
            scheduled.push(*t);
            println!("{} [{}] scheduled task {}", ctx.time(), ctx.id, task.name);
        }
        for t in scheduled {
            self.dag.update_task_state(t, TaskState::Scheduled);
            self.scheduled_tasks.insert(t);
        }
    }
}

#[derive(Debug)]
pub struct Start {}

#[derive(Debug)]
pub struct TaskCompleted {
    task_id: usize,
}

impl Actor for DAGRunner {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        match_event!( event {
            Start {} => {
                self.start(ctx)
            },
            TaskCompleted { task_id } => {
                self.on_task_completed(*task_id, ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
