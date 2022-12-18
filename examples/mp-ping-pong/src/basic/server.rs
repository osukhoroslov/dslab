use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

pub struct BasicPingServer {}

impl Process for BasicPingServer {
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context) {
        if msg.tip == "PING" {
            let resp = Message::new("PONG".to_string(), msg.data);
            ctx.send(resp, from);
        }
    }

    fn on_local_message(&mut self, _msg: Message, _ctx: &mut Context) {}

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) {}
}
