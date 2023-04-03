use std::{cell::RefCell, rc::Rc};

use async_dslab_core::{async_context::AsyncSimulationContext, shared_state::AwaitResult};
use dslab_core::{cast, Event, EventHandler, Id};
use serde::Serialize;

#[derive(Serialize)]
pub struct PingMessage {
    content: String,
}

#[derive(Serialize)]
pub struct StartMessage {
    pub content: String,
}

pub struct Process {
    pub peers: Rc<RefCell<Vec<Id>>>,
    pub ctx: AsyncSimulationContext,
    pub is_pinger: bool,
}

impl Process {
    fn on_start(&mut self, content: String) {
        if self.is_pinger {
            println!("time: {}\t pinger started with content {}", self.ctx.time(), content);
            self.ctx.emit(
                PingMessage {
                    content: "hello".to_string(),
                },
                self.peers.borrow()[0],
                200.,
            );
        } else {
            println!("ponger started with content {}", content);
        }
    }

    fn on_message(&mut self, from: Id, content: String) {
        println!(
            "I am {} receive message from {} with content {}",
            self.ctx.name(),
            from,
            content
        );

        self.ctx.emit(
            PingMessage {
                content: "reply".to_string(),
            },
            from,
            200.,
        );
    }
}

impl EventHandler for Process {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            StartMessage { content } => {
                self.on_start(content);
            }
            PingMessage { content } => {
                self.on_message(event.src, content);
            }
        })
    }
}

pub async fn start_ponger(mut process: Process) {
    let ctx = &mut process.ctx;

    println!("time: {}\t ponger started. Async wait for pinter message", ctx.time());

    let result = ctx
        .async_wait_for_event::<PingMessage>(process.peers.borrow()[0], ctx.id(), 500.)
        .await;

    match result {
        AwaitResult::Ok((e, data)) => {
            println!("time: {}\t got ok message: {}", ctx.time(), data.content);
        }
        AwaitResult::Timeout(e) => println!("time: {}\t event timeouted", ctx.time()),
    }

    ctx.async_wait_for(7.).await;

    println!("time: {}\t ponger after wait", ctx.time());

    println!("ponger finished");
}
