from dslabmp import Context, Message, Process


class DataClass:
    def __init__(self, x=42):
        self.data = x

class TestProcess(Process):
    def __init__(self):
        self.data = ["elem1", (2, 3), {'key': 'value'}, {1, 2, 3}]
        self.messages = [Message('GET', '""')]
        self.inner_member = DataClass()
        self.tmp_value = None

    def on_local_message(self, msg: Message, ctx: Context):
        assert type(self.data) == list
        assert type(self.data[0]) == str
        assert type(self.data[1]) == tuple
        assert type(self.data[2]) == dict
        assert type(self.data[3]) == set
        assert type(self.messages) == list
        assert type(self.messages[0]) == Message
        assert self.inner_member.data == 42
        
        assert self.tmp_value is None
        self.tmp_value = 'CREATED AFTER GET_STATE, SO SHOULD BE DROPPED AFTER SET_STATE'

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from server
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
