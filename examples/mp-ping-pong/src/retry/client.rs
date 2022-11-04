use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

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
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) {
        if msg.tip == "PONG" {
            self.ping = None;
            ctx.cancel_timer("check-pong");
            ctx.send_local(msg);
        }
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) {
        if msg.tip == "PING" {
            self.ping = Some(msg.clone());
            ctx.send(msg, self.server.clone());
            ctx.set_timer("check-pong", 3.);
        }
    }

    fn on_timer(&mut self, timer: String, ctx: &mut Context) {
        if timer == "check-pong" {
            ctx.send(self.ping.as_ref().unwrap().clone(), self.server.clone());
            ctx.set_timer("check-pong", 3.);
        }
    }
}
