from dslabmp import Context, Message, Process


class DataClass:
    def __init__(self, x=42):
        self.data = x

class TestProcess(Process):
    def __init__(self, node_id: str):
        self.data = ["elem1", (2, 3), {'key': 'value'}]
        self._id = node_id
        self.messages = [Message('GET', '""')]
        self.inner_member = DataClass()

        self._id = 'new_node_id'

    def on_local_message(self, msg: Message, ctx: Context):
        assert self._id == 'new_node_id'
        assert self.messages is not None
        assert type(self.messages) == list
        assert type(self.messages[0]) == Message
        assert self.inner_member.data == 42
        tmp_value = "SHOULD BE DROPPED"
        if self.data == tmp_value:
            ctx.send_local(msg)

        # we suppose it will be discarded later
        self.data = tmp_value

        if msg.type == 'FIRST_STEP':
            # we suppose it will be discarded later
            self.data = tmp_value
        elif msg.type == 'SECOND_STEP':
            assert self.data != tmp_value, 'assignments that happened after serialization should be forgottten'

        try:
            a = self.notexists
        except AttributeError:
            return
        except:
            raise 'Not a correct exception raised when addressing not-existing member'
        raise 'No exception raised when addressing not-existing member'

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from server
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
