use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

pub struct BasicPingClient {
    server: String,
}

impl BasicPingClient {
    #[allow(dead_code)]
    pub fn new(server: String) -> Self {
        Self { server }
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
