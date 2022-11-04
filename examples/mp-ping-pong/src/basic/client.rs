use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

pub struct BasicPingClient {
    server: String,
}

impl BasicPingClient {
    pub fn new(server: &str) -> Self {
        Self {
            server: server.to_string(),
        }
    }
}

impl Process for BasicPingClient {
    fn on_message(&mut self, msg: Message, _from: String, ctx: &mut Context) {
        if msg.tip == "PONG" {
            ctx.send_local(msg);
        }
    }

    fn on_local_message(&mut self, msg: Message, ctx: &mut Context) {
        if msg.tip == "PING" {
            ctx.send(msg, self.server.clone());
        }
    }

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}
}
