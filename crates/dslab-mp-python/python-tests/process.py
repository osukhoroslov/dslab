from dslabmp import Context, Message, Process, StateMember


class DataClass:
    def __init__(self, x=42):
        self.data = x

    def serialize(self):
        return {'value': self.data}

    @staticmethod
    def deserialize(data):
        return DataClass(data['value'])


class TestProcess(Process):
    def __init__(self, node_id: str):
        self.data = StateMember(["elem1", (2, 3), {'key': 'value'}])
        self._id = StateMember(node_id)
        self.messages = StateMember([Message('GET', '""')])
        self.inner_member = StateMember(DataClass())

        # examples of set/get state member
        self._id = 'new_node_id'
        self.secret = '''YOU SHOULDN'T SEE IT AFTER RELOAD,
                         IT IS NOT A STATE MEMBER OF NODE %s''' % self._id

    def on_local_message(self, msg: Message, ctx: Context):
        assert self._id == 'new_node_id', 'get/set attributes work bad with state members'
        assert type(self.__dict__[
                    'data']) == StateMember, 'field "data" must be a StateMember both before/after assignment'
        assert self.messages is not None
        assert type(self.messages) == list
        assert type(self.messages[0]) == Message
        assert self.inner_member.data == 42
        tmp_value = "SHOULD BE DROPPED"
        
        if msg.type == 'FIRST_STEP':
            # we suppose it will be discarded later
            self.data = tmp_value
            assert self.secret is not None, 'secret is still present'
        elif msg.type == 'SECOND_STEP':
            assert self.data != tmp_value, 'assignments that happened after serialization should be forgottten'
            assert self.secret is None, 'secret should be forgotten'

        try:
            a = self.notexists
        except AttributeError:
            return
        except:
            raise 'Incorrect exception raised when addressing not-existing member'
        raise 'No exception raised when addressing not-existing member'

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from server
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
