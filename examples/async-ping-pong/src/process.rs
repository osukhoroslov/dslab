use async_dslab_core::async_context::AsyncSimulationContext;
use serde::Serialize;

#[derive(Serialize)]
pub struct PingMessage {
    content: String,
}

// pub struct Process {
//     peers: Vec<Id>,
//     ctx: AsyncSimulationContext,
// }


pub async fn start_pinger(mut ctx: AsyncSimulationContext) {
    println!("pinger started! Time: {}", ctx.time());

    ctx.async_wait_for(42.).await;

    println!("pinger is now at time: {}", ctx.time());

    ctx.emit(PingMessage{content: "hello".to_string()}, ctx., delay)
    println!("pinger is not at time: {}", ctx.time());
}

pub async fn start_ponger(mut ctx: AsyncSimulationContext) {
    println!("ponger started!");

    ctx.async_wait_for(7.).await;

    println!("pinger is now at time: {}", ctx.time());

    ctx.async_wait_for(100.).await;

    println!("pinger is not at time: {}", ctx.time());
}
