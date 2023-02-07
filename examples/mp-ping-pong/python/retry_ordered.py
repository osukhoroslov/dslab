from dslabmp import Context, Message, Process, StateMember
import json

class PingClient(Process):
    def __init__(self, node_id: str, server_id: str):
        self._id = StateMember(node_id)
        self._server_id = StateMember(server_id)
        self._ping = StateMember([])

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'PING':
            self._ping.append(msg)
            if len(self._ping) == 1:
                ctx.send(msg, self._server_id)
                ctx.set_timer('check_pong', 3)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from server
        if msg.type == 'PONG' and len(self._ping) > 0 and self._ping[0]['value'] == msg['value']:
            self._ping = self._ping[1:]
            if len(self._ping) == 0:
                ctx.cancel_timer('check_pong')
            ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name == 'check_pong' and len(self._ping) > 0:
            ctx.send(self._ping[0], self._server_id)
            ctx.set_timer('check_pong', 3)

class PingServer(Process):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from client
        pong = Message('PONG', {'value': msg['value']})
        ctx.send(pong, sender)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
