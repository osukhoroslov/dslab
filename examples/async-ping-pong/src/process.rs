use std::{cell::RefCell, rc::Rc};

use async_dslab_core::{async_context::AsyncSimulationContext, shared_state::AwaitResult};
use dslab_core::Id;
use serde::Serialize;

#[derive(Serialize)]
pub struct PingMessage {
    content: String,
}

pub struct Process {
    pub peers: Rc<RefCell<Vec<Id>>>,
    pub ctx: AsyncSimulationContext,
}

pub async fn start_pinger(mut process: Process) {
    let ctx = &mut process.ctx;

    println!("pinger started! Time: {}", ctx.time());

    ctx.async_wait_for(42.).await;

    println!("pinger is now at time: {}", ctx.time());

    ctx.emit(
        PingMessage {
            content: "hello".to_string(),
        },
        process.peers.borrow()[0],
        200.,
    );
    println!("pinger is not at time: {}", ctx.time());
}

pub async fn start_ponger(mut process: Process) {
    let ctx = &mut process.ctx;

    println!("ponger started!");

    let result = ctx
        .async_wait_for_event::<PingMessage>(process.peers.borrow()[0], ctx.id(), 1000.)
        .await;

    match result {
        AwaitResult::Ok(e) => {
            let pm = e.data.downcast::<PingMessage>();
            match pm {
                Ok(val) => println!("got ok message: {}", val.content),
                Err(..) => panic!("not a ping message"),
            };
        }
        AwaitResult::Timeout(e) => println!("got timeout message"),
    }

    ctx.async_wait_for(7.).await;

    println!("ponger is now at time: {}", ctx.time());

    ctx.async_wait_for(100.).await;

    println!("ponger is not at time: {}", ctx.time());
}
