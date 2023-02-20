from dslabmp import Context, Message, Process

class GroupMember(Process):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'JOIN':
            # Add local node to the group
            seed = msg['seed']
            if seed == self._id:
                # create new empty group and add local node to it
                pass
            else:
                # join existing group
                pass
        elif msg.type == 'LEAVE':
            # Remove local node from the group
            pass
        elif msg.type == 'GET_MEMBERS':
            # Get a list of group members
            # - return the list of all known alive nodes in MEMBERS message
            ctx.send_local(Message('MEMBERS', {'members': [self._id]}))

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # Implement node-to-node communication using any message types
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        pass
