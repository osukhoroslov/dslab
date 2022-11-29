from dslabmp import Context, Message, Process

class GroupMember(Process):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
