use dslab_mp::context::Context;
use dslab_mp::message::Message;
use dslab_mp::process::Process;

#[derive(Clone)]
pub struct BasicPingServer {}

impl Process for BasicPingServer {
    fn on_message(&mut self, msg: Message, from: String, ctx: &mut Context) -> Result<(), String> {
        if msg.tip == "PING" {
            let resp = Message::new("PONG".to_string(), msg.data);
            ctx.send(resp, from);
        }
        Ok(())
    }

    fn on_local_message(&mut self, _msg: Message, _ctx: &mut Context) -> Result<(), String> {
        Ok(())
    }

    fn on_timer(&mut self, _timer: String, _ctx: &mut Context) -> Result<(), String> {
        Ok(())
    }
}
