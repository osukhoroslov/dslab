use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

#[derive(Clone)]
pub struct RetryPingClient {
    server: String,
    ping: Option<Message>,
}

impl RetryPingClient {
    pub fn new(server: &str) -> Self {
        Self {
            server: server.to_string(),
            ping: None,
        }
    }
}

impl Process for RetryPingClient {
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) -> Result<(), String> {
        if msg.tip == "PONG" {
            self.ping = None;
            ctx.cancel_timer("check-pong");
            ctx.send_local(msg);
        }
        Ok(())
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) -> Result<(), String> {
        if msg.tip == "PING" {
            self.ping = Some(msg.clone());
            ctx.send(msg, self.server.clone());
            ctx.set_timer("check-pong", 3.);
        }
        Ok(())
    }

    fn on_timer(&mut self, timer: String, ctx: &mut Context) -> Result<(), String> {
        if timer == "check-pong" {
            ctx.send(self.ping.as_ref().unwrap().clone(), self.server.clone());
            ctx.set_timer("check-pong", 3.);
        }
        Ok(())
    }
}
